"""SQLite 持久化层。

授权族、刷新凭证与轮换记录全部落盘到 SQLite 文件（挂载卷），
因此服务进程/容器重启后，已提交的轮换结果仍可凭原标识恢复。

轮换记录的幂等键为 (family_id, old_credential_hash, rotation_id)：
轮换标识只在「发起该请求的终端所属授权族 + 被消费的旧凭证」范围内代表
一次稳定操作；不同终端（授权族）即使碰巧使用同一轮换标识，也各自形成
独立的首次轮换结果，互不影响。
"""
import sqlite3
import threading
from contextlib import contextmanager

SCHEMA = """
CREATE TABLE IF NOT EXISTS families (
    family_id         TEXT PRIMARY KEY,
    terminal_id       TEXT NOT NULL UNIQUE,          -- 一个终端同一时刻绑定一个授权族
    status            TEXT NOT NULL DEFAULT 'active', -- active | revoked
    revocation_reason TEXT,
    generation        INTEGER NOT NULL DEFAULT 1,     -- 当前代次，初始为 1
    created_at        TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS credentials (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    family_id       TEXT NOT NULL REFERENCES families(family_id),
    credential_hash TEXT NOT NULL UNIQUE,             -- 仅存哈希，避免明文落库
    generation      INTEGER NOT NULL,
    status          TEXT NOT NULL,                    -- current | rotated | revoked
    created_at      TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS rotations (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    family_id           TEXT NOT NULL REFERENCES families(family_id),
    terminal_id         TEXT NOT NULL,
    rotation_id         TEXT NOT NULL,                -- 客户端提供的稳定轮换标识（授权族维度幂等）
    old_credential_hash TEXT NOT NULL,
    new_credential      TEXT NOT NULL,                -- 后继凭证明文：重放时必须原样取回
    new_credential_hash TEXT NOT NULL,
    new_generation      INTEGER NOT NULL,
    created_at          TEXT NOT NULL,
    -- 幂等键：同一授权族内、同一旧凭证、同一轮换标识只落一条轮换记录；
    -- 不同授权族使用同一轮换标识各自独立，互不冲突。
    UNIQUE (family_id, old_credential_hash, rotation_id)
);
"""

# 授权族维度的幂等键（当前 schema）；旧版库文件上是全局 UNIQUE(rotation_id)。
ROTATIONS_KEY = ("family_id", "old_credential_hash", "rotation_id")


class Storage:
    """单连接 + 进程级锁；所有写操作在 BEGIN IMMEDIATE 事务中完成。"""

    def __init__(self, path):
        self._conn = sqlite3.connect(path, check_same_thread=False, isolation_level=None)
        self._conn.row_factory = sqlite3.Row
        self._lock = threading.RLock()
        with self._lock:
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.execute("PRAGMA synchronous=FULL")
            self._conn.execute("PRAGMA busy_timeout=5000")
            self._conn.execute("PRAGMA foreign_keys=ON")
            self._conn.executescript(SCHEMA)
            self._migrate_rotations_key()
            self._heal_interrupted_rotations()

    def close(self):
        with self._lock:
            self._conn.close()

    @contextmanager
    def transaction(self):
        """串行化的事务块：提交后数据立即可靠，异常则整体回滚。"""
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                yield self
            except BaseException:
                self._conn.execute("ROLLBACK")
                raise
            else:
                self._conn.execute("COMMIT")

    # ---- 启动迁移：旧版全局 UNIQUE(rotation_id) -> 授权族维度幂等键 ----

    def _rotations_unique_keys(self):
        """rotations 表上各唯一索引的列组合（用于识别旧版全局唯一约束）。"""
        keys = []
        for idx in self._conn.execute("PRAGMA index_list('rotations')").fetchall():
            if not idx["unique"]:
                continue
            cols = tuple(
                r["name"]
                for r in self._conn.execute(f'PRAGMA index_info("{idx["name"]}")').fetchall()
            )
            keys.append(cols)
        return keys

    def _migrate_rotations_key(self):
        """旧版库文件的 rotations 表把 rotation_id 设为全局唯一，导致不同授权族
        共用同一轮换标识时互相干扰。打开时重建为授权族维度幂等键，数据原样保留。"""
        if ROTATIONS_KEY in self._rotations_unique_keys():
            return
        with self.transaction():
            self._conn.execute(
                "CREATE TABLE rotations_migrated ("
                " id INTEGER PRIMARY KEY AUTOINCREMENT,"
                " family_id TEXT NOT NULL REFERENCES families(family_id),"
                " terminal_id TEXT NOT NULL,"
                " rotation_id TEXT NOT NULL,"
                " old_credential_hash TEXT NOT NULL,"
                " new_credential TEXT NOT NULL,"
                " new_credential_hash TEXT NOT NULL,"
                " new_generation INTEGER NOT NULL,"
                " created_at TEXT NOT NULL,"
                " UNIQUE (family_id, old_credential_hash, rotation_id))"
            )
            self._conn.execute(
                "INSERT INTO rotations_migrated (family_id, terminal_id, rotation_id,"
                " old_credential_hash, new_credential, new_credential_hash,"
                " new_generation, created_at)"
                " SELECT family_id, terminal_id, rotation_id, old_credential_hash,"
                " new_credential, new_credential_hash, new_generation, created_at"
                " FROM rotations"
            )
            self._conn.execute("DROP TABLE rotations")
            self._conn.execute("ALTER TABLE rotations_migrated RENAME TO rotations")
        print("[storage] rotations 幂等键已迁移为 (family_id, old_credential_hash, rotation_id)",
              flush=True)

    # ---- 启动收敛：修复旧实现留下的「中断轮换」状态 ----

    def _heal_interrupted_rotations(self):
        """把旧实现跨族标识冲突留下的中断状态安全收敛为可继续轮换的状态。

        旧实现中，不同授权族共用同一轮换标识时，后到的轮换会写入半截状态：
        旧凭证已置 rotated、后继凭证已插入、轮换记录却因全局唯一约束插入失败、
        代次未推进；之后的重传还会被误判为「凭证重用」而撤销整个授权族。

        收敛规则（只触动没有任何轮换记录背书的半截写入；已有轮换记录背书的
        结果 —— 包括首先成功终端的既有结果 —— 一律保留）：
        - 删除泄漏的后继凭证：代次 >1 却无轮换记录背书，其明文从未送达任何
          客户端，删除不影响任何一方已持有的凭证；
        - 悬空旧凭证恢复为 current：它被半截轮换置为 rotated（或随误判撤销
          置为 revoked），但不存在消费它的轮换记录，即从未真正完成轮换，
          恢复后授权族可凭它继续完成轮换；
        - 解除误判撤销：被中断的授权族并未发生真实的凭证重用，恢复 active
          并清除撤销原因；真实重用导致的撤销（所有已轮换凭证都有轮换记录
          背书）不受影响。
        """
        healed = []
        with self.transaction():
            families = self._conn.execute("SELECT * FROM families").fetchall()
            for fam in families:
                fid = fam["family_id"]
                creds = self._conn.execute(
                    "SELECT * FROM credentials WHERE family_id = ?", (fid,)
                ).fetchall()
                rotations = self._conn.execute(
                    "SELECT old_credential_hash, new_credential_hash FROM rotations"
                    " WHERE family_id = ?", (fid,)
                ).fetchall()
                consumed = {r["old_credential_hash"] for r in rotations}
                produced = {r["new_credential_hash"] for r in rotations}
                leaked = [c for c in creds
                          if c["generation"] > 1
                          and c["credential_hash"] not in produced
                          and c["credential_hash"] not in consumed]
                if not leaked:
                    continue  # 该授权族状态一致，无需收敛
                parent_gens = {c["generation"] - 1 for c in leaked}
                for c in leaked:
                    self._conn.execute("DELETE FROM credentials WHERE id = ?", (c["id"],))
                for c in creds:
                    if c["credential_hash"] in consumed:
                        # 有轮换记录背书的已轮换凭证：恢复正常轮换后的状态
                        if c["status"] != "rotated":
                            self._conn.execute(
                                "UPDATE credentials SET status = 'rotated' WHERE id = ?",
                                (c["id"],))
                    elif c["generation"] in parent_gens:
                        # 悬空旧凭证：恢复为 current，轮换可凭它继续完成
                        self._conn.execute(
                            "UPDATE credentials SET status = 'current' WHERE id = ?",
                            (c["id"],))
                was_revoked = fam["status"] == "revoked"
                if was_revoked:
                    self._conn.execute(
                        "UPDATE families SET status = 'active', revocation_reason = NULL"
                        " WHERE family_id = ?", (fid,))
                healed.append((fam["terminal_id"], fid, was_revoked))
        for terminal_id, fid, was_revoked in healed:
            note = "，并解除误判撤销" if was_revoked else ""
            print(f"[storage] 已收敛授权族 {fid}（终端 {terminal_id}）的中断轮换状态{note}",
                  flush=True)

    # ---- families ----

    def insert_family(self, family_id, terminal_id, created_at):
        self._conn.execute(
            "INSERT INTO families (family_id, terminal_id, status, generation, created_at)"
            " VALUES (?, ?, 'active', 1, ?)",
            (family_id, terminal_id, created_at),
        )

    def find_family_by_terminal(self, terminal_id):
        row = self._conn.execute(
            "SELECT * FROM families WHERE terminal_id = ?", (terminal_id,)
        ).fetchone()
        return dict(row) if row else None

    def find_family(self, family_id):
        row = self._conn.execute(
            "SELECT * FROM families WHERE family_id = ?", (family_id,)
        ).fetchone()
        return dict(row) if row else None

    def set_family_generation(self, family_id, generation):
        self._conn.execute(
            "UPDATE families SET generation = ? WHERE family_id = ?",
            (generation, family_id),
        )

    def revoke_family(self, family_id, reason):
        self._conn.execute(
            "UPDATE families SET status = 'revoked', revocation_reason = ?"
            " WHERE family_id = ?",
            (reason, family_id),
        )

    # ---- credentials ----

    def insert_credential(self, family_id, credential_hash, generation, status, created_at):
        self._conn.execute(
            "INSERT INTO credentials (family_id, credential_hash, generation, status, created_at)"
            " VALUES (?, ?, ?, ?, ?)",
            (family_id, credential_hash, generation, status, created_at),
        )

    def find_credential(self, family_id, credential_hash):
        row = self._conn.execute(
            "SELECT * FROM credentials WHERE family_id = ? AND credential_hash = ?",
            (family_id, credential_hash),
        ).fetchone()
        return dict(row) if row else None

    def set_credential_status(self, credential_id, status):
        self._conn.execute(
            "UPDATE credentials SET status = ? WHERE id = ?", (status, credential_id)
        )

    def revoke_all_credentials(self, family_id):
        self._conn.execute(
            "UPDATE credentials SET status = 'revoked' WHERE family_id = ?", (family_id,)
        )

    # ---- rotations ----

    def insert_rotation(self, family_id, terminal_id, rotation_id,
                        old_credential_hash, new_credential, new_credential_hash,
                        new_generation, created_at):
        self._conn.execute(
            "INSERT INTO rotations (family_id, terminal_id, rotation_id,"
            " old_credential_hash, new_credential, new_credential_hash, new_generation, created_at)"
            " VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (family_id, terminal_id, rotation_id, old_credential_hash,
             new_credential, new_credential_hash, new_generation, created_at),
        )

    def find_rotation_by_old_hash(self, family_id, old_credential_hash):
        row = self._conn.execute(
            "SELECT * FROM rotations WHERE family_id = ? AND old_credential_hash = ?",
            (family_id, old_credential_hash),
        ).fetchone()
        return dict(row) if row else None

    def count_rotations(self, family_id):
        row = self._conn.execute(
            "SELECT COUNT(*) AS n FROM rotations WHERE family_id = ?", (family_id,)
        ).fetchone()
        return row["n"]
