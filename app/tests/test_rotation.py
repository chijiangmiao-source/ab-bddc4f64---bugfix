"""授权族轮换核心逻辑的单元测试（标准库 unittest，无第三方依赖）。"""
import os
import sqlite3
import sys
import tempfile
import threading
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import service  # noqa: E402
from storage import Storage  # noqa: E402


class RotationTestCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.tmp.name, "test.db")
        self.storage = Storage(self.db_path)

    def tearDown(self):
        self.storage.close()
        self.tmp.cleanup()

    def _family(self, terminal="term-1"):
        return service.create_family(self.storage, terminal)

    # ---- 创建 ----

    def test_create_family_issues_generation_one_credential(self):
        fam = self._family()
        self.assertEqual(fam["generation"], 1)
        self.assertTrue(fam["credential"].startswith("rft_"))
        self.assertEqual(fam["family_status"], "active")

    def test_terminal_can_bind_only_one_family(self):
        self._family("term-dup")
        with self.assertRaises(service.TerminalAlreadyBoundError):
            self._family("term-dup")

    def test_create_family_requires_terminal_id(self):
        with self.assertRaises(ValueError):
            self._family("  ")

    # ---- 首次轮换 ----

    def test_first_rotation_accepted_and_generation_increments(self):
        fam = self._family()
        res = service.rotate(self.storage, "term-1", fam["credential"], "rot-1")
        self.assertEqual(res["outcome"], "accepted")
        self.assertEqual(res["generation"], 2)
        self.assertTrue(res["credential"].startswith("rft_"))
        self.assertNotEqual(res["credential"], fam["credential"])
        self.assertEqual(res["family_status"], "active")

    # ---- 幂等重放 ----

    def test_replay_returns_identical_successor_without_advancing(self):
        fam = self._family()
        first = service.rotate(self.storage, "term-1", fam["credential"], "rot-1")
        for _ in range(3):
            again = service.rotate(self.storage, "term-1", fam["credential"], "rot-1")
            self.assertEqual(again["outcome"], "replayed")
            self.assertEqual(again["credential"], first["credential"])
            self.assertEqual(again["generation"], first["generation"])
        view = service.get_family_by_terminal(self.storage, "term-1")
        self.assertEqual(view["generation"], 2)
        self.assertEqual(self.storage.count_rotations(fam["family_id"]), 1)

    def test_replay_survives_service_restart(self):
        fam = self._family()
        first = service.rotate(self.storage, "term-1", fam["credential"], "rot-1")
        # 模拟服务重启：关闭并重新打开同一数据库文件。
        self.storage.close()
        self.storage = Storage(self.db_path)
        again = service.rotate(self.storage, "term-1", fam["credential"], "rot-1")
        self.assertEqual(again["outcome"], "replayed")
        self.assertEqual(again["credential"], first["credential"])
        self.assertEqual(again["generation"], first["generation"])

    def test_replay_returns_historical_successor_after_later_rotations(self):
        fam = self._family()
        first = service.rotate(self.storage, "term-1", fam["credential"], "rot-1")
        second = service.rotate(self.storage, "term-1", first["credential"], "rot-2")
        self.assertEqual(second["generation"], 3)
        again = service.rotate(self.storage, "term-1", fam["credential"], "rot-1")
        self.assertEqual(again["outcome"], "replayed")
        self.assertEqual(again["credential"], first["credential"])
        self.assertEqual(again["generation"], 2)

    # ---- 并发同标识 ----

    def test_concurrent_identical_requests_observe_same_result(self):
        fam = self._family()
        barrier = threading.Barrier(2)

        def call():
            barrier.wait()
            return service.rotate(self.storage, "term-1", fam["credential"], "rot-1")

        results = []
        threads = [threading.Thread(target=lambda: results.append(call())) for _ in range(2)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        self.assertEqual(len(results), 2)
        self.assertEqual(sorted(r["outcome"] for r in results), ["accepted", "replayed"])
        self.assertEqual(results[0]["credential"], results[1]["credential"])
        self.assertEqual(results[0]["generation"], results[1]["generation"])
        self.assertEqual(results[0]["generation"], 2)
        view = service.get_family_by_terminal(self.storage, "term-1")
        self.assertEqual(view["generation"], 2)

    # ---- 跨终端同一轮换标识：授权族维度隔离 ----

    def test_same_rotation_id_across_terminals_rotates_independently(self):
        fam1 = self._family("term-A")
        fam2 = self._family("term-B")
        r1 = service.rotate(self.storage, "term-A", fam1["credential"], "rot-shared")
        r2 = service.rotate(self.storage, "term-B", fam2["credential"], "rot-shared")
        # 第二个终端不得被显示为「重放」，也不得拿到第一个终端的后继凭证
        self.assertEqual(r1["outcome"], "accepted")
        self.assertEqual(r2["outcome"], "accepted")
        self.assertNotEqual(r1["credential"], r2["credential"])
        self.assertEqual(r1["generation"], 2)
        self.assertEqual(r2["generation"], 2)
        self.assertNotEqual(r1["family_id"], r2["family_id"])
        # 两个授权族各自只落一条轮换记录，且均保持可用
        self.assertEqual(self.storage.count_rotations(fam1["family_id"]), 1)
        self.assertEqual(self.storage.count_rotations(fam2["family_id"]), 1)
        for terminal in ("term-A", "term-B"):
            view = service.get_family_by_terminal(self.storage, terminal)
            self.assertEqual(view["family_status"], "active")
            self.assertEqual(view["generation"], 2)

    def test_cross_terminal_replay_returns_own_first_result(self):
        fam1 = self._family("term-A")
        fam2 = self._family("term-B")
        r1 = service.rotate(self.storage, "term-A", fam1["credential"], "rot-shared")
        r2 = service.rotate(self.storage, "term-B", fam2["credential"], "rot-shared")
        # 各自以「自己的旧凭证 + 相同标识」重传，只能取回自己的首次结果
        again1 = service.rotate(self.storage, "term-A", fam1["credential"], "rot-shared")
        again2 = service.rotate(self.storage, "term-B", fam2["credential"], "rot-shared")
        self.assertEqual(again1["outcome"], "replayed")
        self.assertEqual(again1["credential"], r1["credential"])
        self.assertEqual(again1["generation"], 2)
        self.assertEqual(again2["outcome"], "replayed")
        self.assertEqual(again2["credential"], r2["credential"])
        self.assertEqual(again2["generation"], 2)
        # 代次均未推进，双方均未撤销
        for terminal in ("term-A", "term-B"):
            view = service.get_family_by_terminal(self.storage, terminal)
            self.assertEqual(view["generation"], 2)
            self.assertEqual(view["family_status"], "active")

    def test_concurrent_same_rotation_id_across_terminals_isolated(self):
        fam1 = self._family("term-A")
        fam2 = self._family("term-B")
        barrier = threading.Barrier(2)

        def call(terminal, credential):
            barrier.wait()
            return service.rotate(self.storage, terminal, credential, "rot-shared")

        results = []
        threads = [
            threading.Thread(target=lambda: results.append(call("term-A", fam1["credential"]))),
            threading.Thread(target=lambda: results.append(call("term-B", fam2["credential"]))),
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # 两个终端各自形成独立的首次轮换结果
        self.assertEqual(sorted(r["outcome"] for r in results), ["accepted", "accepted"])
        self.assertEqual(len({r["credential"] for r in results}), 2)
        self.assertEqual({r["generation"] for r in results}, {2})
        # 并发后各自重传仍只取回自己的首次结果，互不影响
        by_terminal = {r["terminal_id"]: r for r in results}
        for terminal, fam in (("term-A", fam1), ("term-B", fam2)):
            again = service.rotate(self.storage, terminal, fam["credential"], "rot-shared")
            self.assertEqual(again["outcome"], "replayed")
            self.assertEqual(again["credential"], by_terminal[terminal]["credential"])
            view = service.get_family_by_terminal(self.storage, terminal)
            self.assertEqual(view["generation"], 2)
            self.assertEqual(view["family_status"], "active")

    def test_second_terminal_successor_continues_rotating(self):
        fam1 = self._family("term-A")
        fam2 = self._family("term-B")
        r1 = service.rotate(self.storage, "term-A", fam1["credential"], "rot-shared")
        r2 = service.rotate(self.storage, "term-B", fam2["credential"], "rot-shared")
        # 第二个终端的后继凭证可继续正常轮换
        nxt = service.rotate(self.storage, "term-B", r2["credential"], "rot-next")
        self.assertEqual(nxt["outcome"], "accepted")
        self.assertEqual(nxt["generation"], 3)
        self.assertNotEqual(nxt["credential"], r2["credential"])
        # 第一个终端的既有结果不受影响
        again1 = service.rotate(self.storage, "term-A", fam1["credential"], "rot-shared")
        self.assertEqual(again1["outcome"], "replayed")
        self.assertEqual(again1["credential"], r1["credential"])
        self.assertEqual(again1["generation"], 2)

    def test_cross_terminal_results_survive_service_restart(self):
        fam1 = self._family("term-A")
        fam2 = self._family("term-B")
        r1 = service.rotate(self.storage, "term-A", fam1["credential"], "rot-shared")
        r2 = service.rotate(self.storage, "term-B", fam2["credential"], "rot-shared")
        # 模拟服务重启：关闭并重新打开同一数据库文件
        self.storage.close()
        self.storage = Storage(self.db_path)
        again1 = service.rotate(self.storage, "term-A", fam1["credential"], "rot-shared")
        again2 = service.rotate(self.storage, "term-B", fam2["credential"], "rot-shared")
        self.assertEqual(again1["outcome"], "replayed")
        self.assertEqual(again1["credential"], r1["credential"])
        self.assertEqual(again2["outcome"], "replayed")
        self.assertEqual(again2["credential"], r2["credential"])

    # ---- 异标识重放 => 撤销 ----

    def test_reuse_with_different_rotation_id_revokes_family(self):
        fam = self._family()
        first = service.rotate(self.storage, "term-1", fam["credential"], "rot-1")
        res = service.rotate(self.storage, "term-1", fam["credential"], "rot-OTHER")
        self.assertEqual(res["outcome"], "revoked")
        self.assertEqual(res["family_status"], "revoked")
        self.assertIn("reuse", res["revocation_reason"])

        # 此前签发的后继凭证随后同样被拒绝。
        successor = service.rotate(self.storage, "term-1", first["credential"], "rot-2")
        self.assertEqual(successor["outcome"], "revoked")
        self.assertIn("reuse", successor["revocation_reason"])

        # 原（旧凭证, 原轮换标识）重放也被拒绝。
        replay = service.rotate(self.storage, "term-1", fam["credential"], "rot-1")
        self.assertEqual(replay["outcome"], "revoked")

        # 状态查询可见撤销原因。
        view = service.get_family_by_terminal(self.storage, "term-1")
        self.assertEqual(view["family_status"], "revoked")
        self.assertIn("reuse", view["revocation_reason"])

    # ---- 错误路径 ----

    def test_unknown_terminal_rejected(self):
        with self.assertRaises(service.UnknownTerminalError):
            service.rotate(self.storage, "no-such-terminal", "rft_x", "rot-1")

    def test_unknown_credential_rejected(self):
        self._family()
        with self.assertRaises(service.InvalidCredentialError):
            service.rotate(self.storage, "term-1", "rft_not_issued", "rot-1")

    def test_rotate_requires_all_fields(self):
        fam = self._family()
        with self.assertRaises(ValueError):
            service.rotate(self.storage, "term-1", fam["credential"], "")
        with self.assertRaises(ValueError):
            service.rotate(self.storage, "term-1", "", "rot-1")


class InterruptedRotationHealingTestCase(unittest.TestCase):
    """旧版「跨族同一轮换标识」缺陷留下的中断状态，恢复（重开存储）时应安全收敛。"""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.tmp.name, "test.db")
        self.storage = Storage(self.db_path)

    def tearDown(self):
        self.storage.close()
        self.tmp.cleanup()

    def _reopen(self):
        self.storage.close()
        self.storage = Storage(self.db_path)

    def _simulate_interrupted_rotation(self, terminal, revoked=False):
        """复现旧版缺陷的半截写入：旧凭证已置 rotated、泄漏后继已插入（无轮换
        记录背书）、代次未推进；revoked=True 时进一步模拟误判撤销。"""
        fam = service.create_family(self.storage, terminal)
        leaked = "rft_leaked_successor"
        reuse_reason = (
            f"{service.REASON_REUSE}: rotated credential of terminal '{terminal}' "
            "presented again with a different rotation id"
        )
        with self.storage.transaction() as db:
            db._conn.execute(
                "UPDATE credentials SET status = 'rotated' WHERE credential_hash = ?",
                (service._hash(fam["credential"]),))
            db.insert_credential(fam["family_id"], service._hash(leaked), 2,
                                 "current", service._now())
            if revoked:
                db._conn.execute(
                    "UPDATE families SET status = 'revoked', revocation_reason = ?"
                    " WHERE family_id = ?",
                    (reuse_reason, fam["family_id"]))
                db._conn.execute(
                    "UPDATE credentials SET status = 'revoked' WHERE family_id = ?",
                    (fam["family_id"],))
        return fam

    def test_healing_restores_continuable_state(self):
        fam = self._simulate_interrupted_rotation("term-B")
        self._reopen()  # 恢复时收敛
        view = service.get_family_by_terminal(self.storage, "term-B")
        self.assertEqual(view["family_status"], "active")
        self.assertEqual(view["generation"], 1)
        # 可凭原凭证 + 原轮换标识继续完成轮换
        res = service.rotate(self.storage, "term-B", fam["credential"], "rot-shared")
        self.assertEqual(res["outcome"], "accepted")
        self.assertEqual(res["generation"], 2)
        # 此后重传幂等取回同一后继
        again = service.rotate(self.storage, "term-B", fam["credential"], "rot-shared")
        self.assertEqual(again["outcome"], "replayed")
        self.assertEqual(again["credential"], res["credential"])
        self.assertEqual(again["generation"], 2)

    def test_healing_lifts_false_revocation(self):
        fam = self._simulate_interrupted_rotation("term-B", revoked=True)
        self._reopen()
        # 未发生真实凭证重用：误判撤销被解除，授权族可继续轮换
        view = service.get_family_by_terminal(self.storage, "term-B")
        self.assertEqual(view["family_status"], "active")
        self.assertIsNone(view["revocation_reason"])
        res = service.rotate(self.storage, "term-B", fam["credential"], "rot-shared")
        self.assertEqual(res["outcome"], "accepted")
        self.assertEqual(res["generation"], 2)

    def test_healing_keeps_genuine_revocation(self):
        # 真实重用导致的撤销不得被收敛逻辑解除
        fam_c = service.create_family(self.storage, "term-C")
        first = service.rotate(self.storage, "term-C", fam_c["credential"], "rot-1")
        service.rotate(self.storage, "term-C", fam_c["credential"], "rot-OTHER")
        # 同库内存在一个被中断（并被误判撤销）的授权族
        self._simulate_interrupted_rotation("term-B", revoked=True)
        self._reopen()
        view = service.get_family_by_terminal(self.storage, "term-C")
        self.assertEqual(view["family_status"], "revoked")
        self.assertIn("reuse", view["revocation_reason"])
        res = service.rotate(self.storage, "term-C", first["credential"], "rot-2")
        self.assertEqual(res["outcome"], "revoked")
        # 被误判撤销的授权族则已恢复可用
        view_b = service.get_family_by_terminal(self.storage, "term-B")
        self.assertEqual(view_b["family_status"], "active")

    def test_healing_preserves_successful_family_results(self):
        fam1 = service.create_family(self.storage, "term-A")
        r1 = service.rotate(self.storage, "term-A", fam1["credential"], "rot-shared")
        self._simulate_interrupted_rotation("term-B")
        self._reopen()
        # 原本成功终端的既有结果不被改动
        again = service.rotate(self.storage, "term-A", fam1["credential"], "rot-shared")
        self.assertEqual(again["outcome"], "replayed")
        self.assertEqual(again["credential"], r1["credential"])
        self.assertEqual(again["generation"], 2)
        self.assertEqual(self.storage.count_rotations(fam1["family_id"]), 1)


class LegacySchemaMigrationTestCase(unittest.TestCase):
    """旧版全局 UNIQUE(rotation_id) 的库文件，打开时迁移为授权族维度幂等键并收敛。"""

    LEGACY_SCHEMA = """
    CREATE TABLE families (
        family_id TEXT PRIMARY KEY, terminal_id TEXT NOT NULL UNIQUE,
        status TEXT NOT NULL DEFAULT 'active', revocation_reason TEXT,
        generation INTEGER NOT NULL DEFAULT 1, created_at TEXT NOT NULL
    );
    CREATE TABLE credentials (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        family_id TEXT NOT NULL REFERENCES families(family_id),
        credential_hash TEXT NOT NULL UNIQUE,
        generation INTEGER NOT NULL, status TEXT NOT NULL, created_at TEXT NOT NULL
    );
    CREATE TABLE rotations (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        family_id TEXT NOT NULL REFERENCES families(family_id),
        terminal_id TEXT NOT NULL, rotation_id TEXT NOT NULL,
        old_credential_hash TEXT NOT NULL, new_credential TEXT NOT NULL,
        new_credential_hash TEXT NOT NULL, new_generation INTEGER NOT NULL,
        created_at TEXT NOT NULL,
        UNIQUE (rotation_id)
    );
    """

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.tmp.name, "test.db")

    def tearDown(self):
        self.tmp.cleanup()

    def test_legacy_global_unique_schema_migrated_and_healed(self):
        # 用旧版 schema 建库：term-A 正常落轮换记录；term-B 留下半截写入。
        conn = sqlite3.connect(self.db_path)
        conn.executescript(self.LEGACY_SCHEMA)
        now = service._now()
        cred_a0, cred_a1, cred_b0, leaked_b1 = (
            "rft_a0", "rft_a1", "rft_b0", "rft_leaked_b1")
        conn.execute("INSERT INTO families VALUES ('fam_a', 'term-A', 'active', NULL, 2, ?)",
                     (now,))
        conn.execute("INSERT INTO credentials (family_id, credential_hash, generation, status, created_at)"
                     " VALUES ('fam_a', ?, 1, 'rotated', ?)", (service._hash(cred_a0), now))
        conn.execute("INSERT INTO credentials (family_id, credential_hash, generation, status, created_at)"
                     " VALUES ('fam_a', ?, 2, 'current', ?)", (service._hash(cred_a1), now))
        conn.execute("INSERT INTO rotations (family_id, terminal_id, rotation_id,"
                     " old_credential_hash, new_credential, new_credential_hash,"
                     " new_generation, created_at)"
                     " VALUES ('fam_a', 'term-A', 'rot-shared', ?, ?, ?, 2, ?)",
                     (service._hash(cred_a0), cred_a1, service._hash(cred_a1), now))
        conn.execute("INSERT INTO families VALUES ('fam_b', 'term-B', 'active', NULL, 1, ?)",
                     (now,))
        conn.execute("INSERT INTO credentials (family_id, credential_hash, generation, status, created_at)"
                     " VALUES ('fam_b', ?, 1, 'rotated', ?)", (service._hash(cred_b0), now))
        conn.execute("INSERT INTO credentials (family_id, credential_hash, generation, status, created_at)"
                     " VALUES ('fam_b', ?, 2, 'current', ?)", (service._hash(leaked_b1), now))
        conn.commit()
        conn.close()

        storage = Storage(self.db_path)  # 打开即触发迁移 + 收敛
        try:
            # 幂等键已迁移为授权族维度
            keys = [
                tuple(r["name"] for r in storage._conn.execute(
                    f'PRAGMA index_info("{idx["name"]}")').fetchall())
                for idx in storage._conn.execute("PRAGMA index_list('rotations')").fetchall()
                if idx["unique"]
            ]
            self.assertIn(("family_id", "old_credential_hash", "rotation_id"), keys)
            # term-B 已收敛：可凭原凭证 + 相同轮换标识完成自己的首次轮换
            res = service.rotate(storage, "term-B", cred_b0, "rot-shared")
            self.assertEqual(res["outcome"], "accepted")
            self.assertEqual(res["generation"], 2)
            self.assertNotEqual(res["credential"], cred_a1)
            # term-A 的既有结果不受影响：重放仍取回原后继
            again = service.rotate(storage, "term-A", cred_a0, "rot-shared")
            self.assertEqual(again["outcome"], "replayed")
            self.assertEqual(again["credential"], cred_a1)
            self.assertEqual(again["generation"], 2)
            self.assertEqual(storage.count_rotations("fam_a"), 1)
            self.assertEqual(storage.count_rotations("fam_b"), 1)
        finally:
            storage.close()


if __name__ == "__main__":
    unittest.main()
