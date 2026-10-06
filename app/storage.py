"""SQLite 持久化层。

授权族、刷新凭证与轮换记录全部落盘到 SQLite 文件（挂载卷），
因此服务进程/容器重启后，已提交的轮换结果仍可凭原标识恢复。
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
    rotation_id         TEXT NOT NULL,                -- 客户端提供的稳定轮换标识（仅在同一授权族内幂等）
    old_credential_hash TEXT NOT NULL,
    new_credential      TEXT NOT NULL,                -- 后继凭证明文：重放时必须原样取回
    new_credential_hash TEXT NOT NULL,
    new_generation      INTEGER NOT NULL,
    created_at          TEXT NOT NULL,
    -- 轮换标识只在「发起它的终端 / 旧凭证」范围内代表一次稳定操作；
    -- 不同授权族（乃至不同旧凭证）碰巧使用相同标识必须互不影响。
    UNIQUE (family_id, old_credential_hash, rotation_id)
);
"""

_ROTATION_COLUMNS = (
    "id, family_id, terminal_id, rotation_id, old_credential_hash, "
    "new_credential, new_credential_hash, new_generation, created_at"
)

_SCOPED_INDEX = "ux_rotations_family_oldcred_rotation"


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
            self._migrate_rotations_scope()
            self._heal_corrupted_families()

    def _migrate_rotations_scope(self):
        """旧库 rotations 为全局 UNIQUE(rotation_id)：重建为族内范围唯一。

        旧约束会令不同授权族的同标识轮换互相误伤；已落库的轮换记录本身有效，
        只重建唯一约束范围，不改变任何既有轮换结果。
        """
        table_sql = self._conn.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name='rotations'"
        ).fetchone()
        if table_sql and "UNIQUE (family_id, old_credential_hash, rotation_id)" in table_sql["sql"]:
            return
        index = self._conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='index' AND name=?",
            (_SCOPED_INDEX,),
        ).fetchone()
        if index:
            return  # 已迁移过（CTAS 重建的表 + 范围唯一索引）
        self._conn.executescript(
            "ALTER TABLE rotations RENAME TO rotations_legacy;\n"
            f"CREATE TABLE rotations AS SELECT {_ROTATION_COLUMNS} FROM rotations_legacy;\n"
            "DROP TABLE rotations_legacy;"
        )
        # CTAS 不携带约束，补上正确的范围唯一约束。
        self._conn.execute(
            f"CREATE UNIQUE INDEX {_SCOPED_INDEX} "
            "ON rotations(family_id, old_credential_hash, rotation_id)"
        )

    def _heal_corrupted_families(self):
        """收敛旧版本「跨终端同标识碰撞」造成的半提交/误撤销状态。

        旧版本在全局唯一约束上撞车时会提交半成品：本终端旧凭证被置为 rotated、
        插入了孤立 current 凭证，却没有写入轮换记录；随后重传还会被误判为凭证
        重用而撤销整个授权族。这里仅处理「没有任何轮换记录」的授权族 —— 正常
        完成过轮换的终端与真正因重用被撤销的授权族都不在范围内，绝不改动：

          1. 清除误判的撤销状态与原因；
          2. 删除半提交插入、且无轮换记录指向的孤立 current 凭证；
          3. 把被误置为 rotated 的凭证恢复为 current；
          4. family 代次重算为现存凭证的真实代次。

        修复后终端可凭原旧凭证 + 原标识正常完成首次轮换。
        """
        family_ids = [
            r["family_id"]
            for r in self._conn.execute(
                "SELECT family_id FROM families f WHERE NOT EXISTS ("
                "  SELECT 1 FROM rotations r WHERE r.family_id = f.family_id)"
            ).fetchall()
        ]
        for family_id in family_ids:
            # 零轮换记录的授权族不可能真正触发过重用撤销（撤销必先有轮换），
            # 因此 revoked 必定是误判；半成品凭证也只可能是碰撞后的半提交产物。
            self._conn.execute(
                "UPDATE families SET status='active', revocation_reason=NULL"
                " WHERE family_id=? AND status='revoked'",
                (family_id,),
            )
            # 删除半提交插入的高代次孤立凭证，只保留最低代次的原始旧凭证。
            self._conn.execute(
                "DELETE FROM credentials WHERE family_id=? AND generation > ("
                "  SELECT COALESCE(MIN(generation), 1) FROM credentials WHERE family_id=?)",
                (family_id, family_id),
            )
            # 原始旧凭证可能被误置为 rotated/revoked，恢复为 current。
            self._conn.execute(
                "UPDATE credentials SET status='current' WHERE family_id=?",
                (family_id,),
            )
            self._conn.execute(
                "UPDATE families SET generation=1 WHERE family_id=?", (family_id,)
            )

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

    def find_rotation(self, family_id, rotation_id):
        """按 (授权族, 轮换标识) 找回首次轮换结果 —— 标识只在族内幂等。"""
        row = self._conn.execute(
            "SELECT * FROM rotations WHERE family_id = ? AND rotation_id = ?",
            (family_id, rotation_id),
        ).fetchone()
        return dict(row) if row else None

    def count_rotations(self, family_id):
        row = self._conn.execute(
            "SELECT COUNT(*) AS n FROM rotations WHERE family_id = ?", (family_id,)
        ).fetchone()
        return row["n"]
