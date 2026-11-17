"""Managed reference constraints: registration, deferred commit-time checks,
cascade expansion and restrict semantics.

The expected cascade sets are computed either as explicit literals or by a
tiny independent graph reachability routine defined in this file.  That
routine never imports the product's cascade planner, so the assertions are
independent of the implementation under test.
"""

import os
import tempfile
import unittest

from mrtx import Client, ConflictError, ReferenceError, SchemaError


# ---------------------------------------------------------------------------
# Independent expectation machinery (must not use mrtx.references)
# ---------------------------------------------------------------------------


def independent_cascade(edges, roots):
    """Set-reachability over ``(parent_table, parent_id) -> child identities``.

    ``edges`` maps a parent identity key to a list of
    ``(child_key, constraint_name)`` for cascade constraints only.  Worklist
    semantics with a visited set give de-duplication for diamond and ring
    graphs.  This is the textbook fixpoint the product must reproduce; it is
    written here solely to cross-check the product, never shared with it.
    """
    deleted = set(roots)
    pending = list(roots)
    while pending:
        key = pending.pop(0)
        for child_key, _name in edges.get(key, ()):
            if child_key not in deleted:
                deleted.add(child_key)
                pending.append(child_key)
    return deleted


class RefRepo:
    """Fresh temporary repository with parent/child style managed tables."""

    def __init__(self):
        self.dir = tempfile.mkdtemp(prefix="mrtx-ref-")
        self.path = os.path.join(self.dir, "repo.db")
        self.c = Client(self.path)

    def close(self):
        import shutil
        shutil.rmtree(self.dir, ignore_errors=True)


class RefCase(unittest.TestCase):
    def setUp(self):
        self.rr = RefRepo()
        self.c = self.rr.c

    def tearDown(self):
        self.rr.close()

    def table(self, name, *cols):
        self.c.create_table(name, [(col, "text") for col in cols])

    def insert(self, table, row_id, **values):
        tx = self.c.begin()
        tx.stage_insert(table, row_id, values)
        tx.commit()

    def update(self, table, row_id, **values):
        tx = self.c.begin()
        tx.stage_update(table, row_id, values)
        tx.commit()

    def delete(self, table, row_id):
        tx = self.c.begin()
        tx.stage_delete(table, row_id)
        return tx.commit()

    def ids(self, table):
        return {r["row_id"] for r in self.c.rows(table)}


# ---------------------------------------------------------------------------
# Registration
# ---------------------------------------------------------------------------


class TestRegistration(RefCase):
    def setUp(self):
        super().setUp()
        self.table("parents", "name")
        self.table("children", "parent_id", "label")

    def test_register_and_query(self):
        out = self.c.register_reference(
            "c_p", "parents", "children", "parent_id", "cascade")
        self.assertEqual(out["name"], "c_p")
        self.assertEqual(out["on_delete"], "cascade")
        self.assertEqual(out["registered_version"], 0)
        listed = self.c.list_references()
        self.assertEqual([r["constraint_name"] for r in listed], ["c_p"])
        self.assertEqual(self.c.get_reference("c_p")["child_table"], "children")
        self.assertIsNone(self.c.get_reference("nope"))

    def test_duplicate_name_refused(self):
        self.c.register_reference("c_p", "parents", "children", "parent_id",
                                  "cascade")
        self.table("other", "x")
        with self.assertRaises(ReferenceError):
            self.c.register_reference("c_p", "parents", "other", "x", "restrict")
        # ReferenceError remains a SchemaError-family managed error.
        self.assertTrue(issubclass(ReferenceError, SchemaError))
        self.assertEqual(len(self.c.list_references()), 1)

    def test_duplicate_column_binding_refused(self):
        self.c.register_reference("c_p", "parents", "children", "parent_id",
                                  "cascade")
        with self.assertRaises(SchemaError):
            self.c.register_reference("c_p_2", "parents", "children",
                                      "parent_id", "restrict")
        self.assertEqual(len(self.c.list_references()), 1)

    def test_unknown_table_and_column_refused(self):
        with self.assertRaises(SchemaError):
            self.c.register_reference("x1", "parents", "nope", "parent_id",
                                      "cascade")
        with self.assertRaises(SchemaError):
            self.c.register_reference("x2", "nope", "children", "parent_id",
                                      "cascade")
        with self.assertRaises(SchemaError):
            self.c.register_reference("x3", "parents", "children", "nope",
                                      "cascade")
        self.assertEqual(self.c.list_references(), [])

    def test_non_text_column_refused(self):
        self.c.create_table("num", [("v", "int64")])
        with self.assertRaises(SchemaError):
            self.c.register_reference("x", "parents", "num", "v", "cascade")
        self.assertEqual(self.c.list_references(), [])

    def test_unsupported_strategy_refused(self):
        with self.assertRaises(SchemaError):
            self.c.register_reference("x", "parents", "children", "parent_id",
                                      "set-null")
        with self.assertRaises(SchemaError):
            self.c.register_reference("y", "parents", "children", "parent_id",
                                      None)
        self.assertEqual(self.c.list_references(), [])

    def test_existing_orphan_blocks_registration_with_no_half_definition(self):
        self.insert("parents", "P1", name="p")
        # children with no constraint yet may hold a dangling reference.
        self.insert("children", "good", parent_id="P1", label="g")
        self.insert("children", "bad", parent_id="GONE", label="b")
        with self.assertRaises(SchemaError) as info:
            self.c.register_reference("c_p", "parents", "children",
                                      "parent_id", "cascade")
        self.assertIn("bad", str(info.exception))
        self.assertEqual(self.c.list_references(), [])
        # Repair the data; registration now succeeds.
        self.update("children", "bad", parent_id="P1")
        self.c.register_reference("c_p", "parents", "children", "parent_id",
                                  "cascade")
        self.assertEqual(len(self.c.list_references()), 1)

    def test_null_values_do_not_count_as_orphans(self):
        self.insert("children", "loose", parent_id=None, label="x")
        out = self.c.register_reference("c_p", "parents", "children",
                                        "parent_id", "restrict")
        self.assertEqual(out["name"], "c_p")

    def test_durable_across_process_reopen(self):
        self.c.register_reference("c_p", "parents", "children", "parent_id",
                                  "cascade")
        fresh = Client(self.rr.path)
        self.assertEqual([r["constraint_name"] for r in fresh.list_references()],
                         ["c_p"])
        # The re-opened repository enforces the rule.
        tx = fresh.begin()
        tx.stage_insert("children", "x", {"parent_id": "missing", "label": "l"})
        with self.assertRaises(ConflictError):
            tx.commit()


# ---------------------------------------------------------------------------
# Deferred, final-state reference checking
# ---------------------------------------------------------------------------


class TestDeferredChecking(RefCase):
    def setUp(self):
        super().setUp()
        self.table("parents", "name")
        self.table("children", "parent_id", "label")
        self.c.register_reference("c_p", "parents", "children", "parent_id",
                                  "cascade")

    def test_child_staged_before_parent_in_one_transaction(self):
        tx = self.c.begin()
        tx.stage_insert("children", "k1", {"parent_id": "P1", "label": "a"})
        # A read inside the transaction already sees its own (momentarily
        # orphaned) write; staging never rejects it.
        self.assertEqual([r["row_id"] for r in tx.read("children")], ["k1"])
        tx.stage_insert("parents", "P1", {"name": "one"})
        result = tx.commit()
        self.assertEqual(result["status"], "committed")
        self.assertEqual(result["cascade_deletes"], [])

    def test_transient_orphan_repaired_before_commit(self):
        self.insert("parents", "P1", name="one")
        tx = self.c.begin()
        tx.stage_insert("children", "k", {"parent_id": "MISSING", "label": "x"})
        tx.stage_update("children", "k", {"parent_id": "P1"})
        self.assertEqual(tx.commit()["status"], "committed")

    def test_final_orphan_rejects_and_rolls_back_unrelated_writes(self):
        self.insert("parents", "P1", name="one")
        self.insert("parents", "P2", name="two")
        tx = self.c.begin()
        tx.stage_insert("children", "orphan",
                        {"parent_id": "GHOST", "label": "x"})
        tx.stage_update("parents", "P2", {"name": "changed"})
        with self.assertRaises(ConflictError) as info:
            tx.commit()
        ev = info.exception.evidence
        self.assertEqual(ev["code"], "reference_violation")
        problem = ev["conflicts"][0]
        self.assertEqual(problem["constraint"], "c_p")
        self.assertEqual(problem["table"], "children")
        self.assertEqual(problem["row_id"], "orphan")
        self.assertEqual(problem["missing_parent"], "GHOST")
        # The otherwise-valid staged update did not land.
        self.assertEqual(
            [r for r in self.c.rows("parents") if r["row_id"] == "P2"][0]["name"],
            "two")
        self.assertNotIn("orphan", self.ids("children"))

    def test_repeated_commit_returns_same_durable_decision(self):
        tx = self.c.begin()
        tx.stage_insert("children", "orphan",
                        {"parent_id": "GHOST", "label": "x"})
        with self.assertRaises(ConflictError):
            tx.commit()
        status = self.c.transaction_status(tx.txid)
        self.assertEqual(status["decision"], "rejected")
        with self.assertRaises(ConflictError) as info:
            self.c.commit(tx.handle)
        self.assertTrue(info.exception.evidence["replayed"])
        self.assertEqual(info.exception.code, "reference_violation")

    def test_delete_and_reinsert_parent_same_id_does_not_cascade(self):
        self.insert("parents", "P1", name="one")
        self.insert("children", "k", parent_id="P1", label="a")
        tx = self.c.begin()
        tx.stage_delete("parents", "P1")
        tx.stage_insert("parents", "P1", {"name": "one-reborn"})
        result = tx.commit()
        self.assertEqual(result["cascade_deletes"], [])
        self.assertIn("k", self.ids("children"))
        self.assertEqual(
            self.c.rows("parents")[0]["name"], "one-reborn")

    def test_reparent_child_survives_old_parent_deletion(self):
        self.insert("parents", "P1", name="one")
        self.insert("parents", "P2", name="two")
        self.insert("children", "k", parent_id="P1", label="a")
        tx = self.c.begin()
        tx.stage_update("children", "k", {"parent_id": "P2"})
        tx.stage_delete("parents", "P1")
        result = tx.commit()
        self.assertEqual(result["cascade_deletes"], [])
        self.assertIn("k", self.ids("children"))
        self.assertEqual(self.c.rows("children")[0]["parent_id"], "P2")

    def test_case_sensitive_code_point_matching_no_normalization(self):
        self.insert("parents", "Abc", name="x")
        tx = self.c.begin()
        tx.stage_insert("children", "lower", {"parent_id": "abc", "label": "l"})
        with self.assertRaises(ConflictError):
            tx.commit()
        tx2 = self.c.begin()
        tx2.stage_insert("children", "exact", {"parent_id": "Abc", "label": "e"})
        tx2.commit()
        self.assertIn("exact", self.ids("children"))

    def test_business_null_means_no_reference(self):
        tx = self.c.begin()
        tx.stage_insert("children", "loose", {"parent_id": None, "label": "l"})
        tx.commit()
        # Deleting any parent cannot affect a null-reference row.
        self.insert("parents", "P1", name="x")
        self.delete("parents", "P1")
        self.assertIn("loose", self.ids("children"))


# ---------------------------------------------------------------------------
# Cascade expansion
# ---------------------------------------------------------------------------


class TestCascade(RefCase):
    def test_simple_cascade_records_propagation_source(self):
        self.table("parents", "name")
        self.table("children", "parent_id", "label")
        self.c.register_reference("c_p", "parents", "children", "parent_id",
                                  "cascade")
        self.insert("parents", "P1", name="x")
        self.insert("children", "k1", parent_id="P1", label="a")
        self.insert("children", "k2", parent_id="P1", label="b")
        result = self.delete("parents", "P1")
        casc = result["cascade_deletes"]
        self.assertEqual({(x["table"], x["row_id"]) for x in casc},
                         {("children", "k1"), ("children", "k2")})
        for item in casc:
            self.assertEqual(item["constraint"], "c_p")
            self.assertEqual(item["propagation_source"], ["parents", "P1"])
            self.assertEqual(item["depth"], 1)
            self.assertFalse(item["ephemeral"])
        self.assertEqual(self.ids("parents"), set())
        self.assertEqual(self.ids("children"), set())
        # History keeps the real before-image then one tombstone, both stamped
        # by actual source positions (the tombstone at this commit's position).
        history = self.c.history("children", "k1")
        self.assertEqual([h["deleted"] for h in history], [False, True])
        self.assertEqual(history[-1]["version"], result["commit_version"])
        self.assertEqual(history[0]["version"], casc[0]["old_version"])

    def test_cascade_chain(self):
        # t1 -> t2 -> t3 (each row references the previous table's row).
        self.table("t1", "x")
        self.table("t2", "t1_id")
        self.table("t3", "t2_id")
        self.c.register_reference("e12", "t1", "t2", "t1_id", "cascade")
        self.c.register_reference("e23", "t2", "t3", "t2_id", "cascade")
        self.insert("t1", "A", x="1")
        self.insert("t2", "B", t1_id="A")
        self.insert("t3", "C", t2_id="B")
        result = self.delete("t1", "A")
        got = {(x["table"], x["row_id"]) for x in result["cascade_deletes"]}
        want = independent_cascade(
            {("t1", "A"): [(("t2", "B"), "e12")],
             ("t2", "B"): [(("t3", "C"), "e23")]},
            {("t1", "A")}) - {("t1", "A")}
        self.assertEqual(got, want)
        depths = {(x["table"], x["row_id"]): x["depth"]
                  for x in result["cascade_deletes"]}
        self.assertEqual(depths[("t2", "B")], 1)
        self.assertEqual(depths[("t3", "C")], 2)
        self.assertEqual(self.ids("t1") | self.ids("t2") | self.ids("t3"),
                         set())

    def test_self_reference_loop_terminates(self):
        self.table("nodes", "ref", "kind")
        self.c.register_reference("self_c", "nodes", "nodes", "ref", "cascade")
        # A -> B -> A ring plus a leaf C -> B.  The ring rows are born in one
        # transaction because neither can exist alone under the rule.
        tx = self.c.begin()
        tx.stage_insert("nodes", "A", {"ref": "B", "kind": "a"})
        tx.stage_insert("nodes", "B", {"ref": "A", "kind": "b"})
        tx.commit()
        self.insert("nodes", "C", ref="B", kind="c")
        result = self.delete("nodes", "A")
        got = {(x["table"], x["row_id"]) for x in result["cascade_deletes"]}
        edges = {
            ("nodes", "A"): [(("nodes", "B"), "self_c")],
            ("nodes", "B"): [(("nodes", "A"), "self_c"),
                             (("nodes", "C"), "self_c")],
            ("nodes", "C"): [],
        }
        want = independent_cascade(edges, {("nodes", "A")}) - {("nodes", "A")}
        self.assertEqual(got, want)
        self.assertEqual(got, {("nodes", "B"), ("nodes", "C")})
        # Each identity deleted exactly once.
        self.assertEqual(len(got), len(result["cascade_deletes"]))
        self.assertEqual(self.ids("nodes"), set())

    def test_two_table_ring_terminates(self):
        self.table("a", "b_id")
        self.table("b", "a_id")
        self.c.register_reference("a2b", "b", "a", "b_id", "cascade")
        self.c.register_reference("b2a", "a", "b", "a_id", "cascade")
        self.insert("b", "B1", a_id=None)
        self.insert("a", "A1", b_id="B1")
        self.update("b", "B1", a_id="A1")
        result = self.delete("a", "A1")
        got = {(x["table"], x["row_id"]) for x in result["cascade_deletes"]}
        self.assertEqual(got, {("b", "B1")})
        self.assertEqual(self.ids("a"), set())
        self.assertEqual(self.ids("b"), set())

    def test_diamond_deletes_each_identity_once(self):
        self.table("root", "x")
        self.table("mid", "root_id")
        self.table("leaf", "m1_id", "m2_id")
        self.c.register_reference("rm", "root", "mid", "root_id", "cascade")
        self.c.register_reference("lm1", "mid", "leaf", "m1_id", "cascade")
        self.c.register_reference("lm2", "mid", "leaf", "m2_id", "cascade")
        self.insert("root", "R", x="1")
        self.insert("mid", "M1", root_id="R")
        self.insert("mid", "M2", root_id="R")
        self.insert("leaf", "L", m1_id="M1", m2_id="M2")
        result = self.delete("root", "R")
        got = [(x["table"], x["row_id"]) for x in result["cascade_deletes"]]
        edges = {
            ("root", "R"): [(("mid", "M1"), "rm"), (("mid", "M2"), "rm")],
            ("mid", "M1"): [(("leaf", "L"), "lm1")],
            ("mid", "M2"): [(("leaf", "L"), "lm2")],
            ("leaf", "L"): [],
        }
        want = independent_cascade(edges, {("root", "R")}) - {("root", "R")}
        self.assertEqual(set(got), want)
        # L reached by two paths but deleted once.
        self.assertEqual(len(got), 3)

    def test_row_referencing_two_parents(self):
        self.table("m1", "v")
        self.table("m2", "v")
        self.table("joiner", "a_id", "b_id")
        self.c.register_reference("j1", "m1", "joiner", "a_id", "cascade")
        self.c.register_reference("j2", "m2", "joiner", "b_id", "cascade")
        self.insert("m1", "A", v="1")
        self.insert("m2", "B", v="1")
        self.insert("joiner", "J", a_id="A", b_id="B")
        result = self.delete("m1", "A")
        self.assertEqual(
            [(x["table"], x["row_id"]) for x in result["cascade_deletes"]],
            [("joiner", "J")])
        self.assertIn("B", self.ids("m2"))

    def test_ephemeral_inserted_child_gets_no_fabricated_history(self):
        self.table("pc", "v")
        self.table("cc", "p_id")
        self.c.register_reference("rc", "pc", "cc", "p_id", "cascade")
        tx = self.c.begin()
        tx.stage_insert("pc", "PE", {"v": "1"})
        tx.stage_insert("cc", "CE", {"p_id": "PE"})
        tx.stage_delete("pc", "PE")
        result = tx.commit()
        casc = result["cascade_deletes"]
        self.assertEqual([(x["table"], x["row_id"], x["ephemeral"]) for x in casc],
                         [("cc", "CE", True)])
        self.assertEqual(self.ids("pc"), set())
        self.assertEqual(self.ids("cc"), set())
        # No business row was ever committed for CE: empty history.
        self.assertEqual(self.c.history("cc", "CE"), [])
        # And no explicit write evidence for the suppressed staged insert.
        self.assertNotIn("cc",
                         {w["table"] for w in result["writes"]})

    def test_child_updated_and_cascade_deleted_in_same_transaction(self):
        self.table("p", "v")
        self.table("ch", "p_id", "note")
        self.c.register_reference("rc", "p", "ch", "p_id", "cascade")
        self.insert("p", "P", v="1")
        self.insert("ch", "C", p_id="P", note="a")
        tx = self.c.begin()
        tx.stage_update("ch", "C", {"note": "b"})
        tx.stage_delete("p", "P")
        result = tx.commit()
        # The row's only durable fate is one cascade tombstone; the
        # intermediate update never becomes a visible business state, so there
        # is exactly one history row at the commit position.
        self.assertEqual(
            [(x["table"], x["row_id"]) for x in result["cascade_deletes"]],
            [("ch", "C")])
        history = self.c.history("ch", "C")
        self.assertEqual([h["deleted"] for h in history], [False, True])
        self.assertEqual(history[-1]["version"], result["commit_version"])
        self.assertEqual(history[0]["version"],
                         result["cascade_deletes"][0]["old_version"])
        self.assertEqual(history[0]["note"], "a")  # true prior image preserved
        self.assertEqual(self.ids("ch"), set())

    def test_explicit_and_cascade_deletes_share_one_source_position(self):
        self.table("p", "v")
        self.table("ch", "p_id")
        self.c.register_reference("rc", "p", "ch", "p_id", "cascade")
        self.insert("p", "P", v="1")
        self.insert("ch", "K", p_id="P")
        result = self.delete("p", "P")
        pos = result["commit_version"]
        self.assertTrue(all(w["commit_version"] == pos for w in result["writes"]))
        self.assertTrue(all(x["commit_version"] == pos
                            for x in result["cascade_deletes"]))
        # User delete vs cascade delete are distinguishable.
        self.assertEqual(result["writes"][0]["op"], "delete")
        self.assertEqual(
            result["cascade_deletes"][0]["op"], "cascade_delete")


# ---------------------------------------------------------------------------
# Restrict
# ---------------------------------------------------------------------------


class TestRestrict(RefCase):
    def setUp(self):
        super().setUp()
        self.table("p", "v")
        self.table("ch", "p_id")
        self.c.register_reference("rr", "p", "ch", "p_id", "restrict")

    def test_surviving_referrer_blocks_delete(self):
        self.insert("p", "P", v="1")
        self.insert("ch", "C", p_id="P")
        tx = self.c.begin()
        tx.stage_delete("p", "P")
        with self.assertRaises(ConflictError) as info:
            tx.commit()
        ev = info.exception.evidence
        self.assertEqual(ev["code"], "restrict_violation")
        problem = ev["conflicts"][0]
        self.assertEqual(problem["constraint"], "rr")
        self.assertEqual(problem["table"], "ch")
        self.assertEqual(problem["row_id"], "C")
        self.assertEqual(problem["missing_parent"], "P")
        # Nothing landed.
        self.assertIn("P", self.ids("p"))
        self.assertIn("C", self.ids("ch"))

    def test_referrer_deleted_same_batch_does_not_block(self):
        # The restrict referrer is itself removed by an explicit delete, so the
        # parent delete is legal even though a restrict edge exists.
        self.insert("p", "P", v="1")
        self.insert("ch", "C", p_id="P")
        tx = self.c.begin()
        tx.stage_delete("ch", "C")
        tx.stage_delete("p", "P")
        result = tx.commit()
        self.assertEqual(result["cascade_deletes"], [])
        self.assertEqual(self.ids("p"), set())
        self.assertEqual(self.ids("ch"), set())

    def test_restrict_is_not_silently_treated_as_cascade(self):
        self.insert("p", "P", v="1")
        self.insert("ch", "C", p_id="P")
        tx = self.c.begin()
        tx.stage_delete("p", "P")
        with self.assertRaises(ConflictError) as info:
            tx.commit()
        self.assertEqual(info.exception.code, "restrict_violation")
        # Child must still be present (no implicit deletion happened).
        self.assertIn("C", self.ids("ch"))

    def test_restrict_after_cascade_set_is_known(self):
        # Graph: root --cascade--> mid --restrict--> leaf, and mid also reaches
        # leaf through a second cascade path.  Deleting root removes mid and
        # leaf via cascade; the restrict edge must not fire because its
        # referrer (leaf) is inside the final cascade set.  This proves
        # restrict is evaluated only after the complete cascade set is known.
        self.table("root", "x")
        self.table("mid", "root_id")
        self.table("leaf", "mid_id", "mid2_id")
        self.c.register_reference("rm", "root", "mid", "root_id", "cascade")
        self.c.register_reference("lm_r", "mid", "leaf", "mid_id", "restrict")
        self.c.register_reference("lm_c", "mid", "leaf", "mid2_id", "cascade")
        self.insert("root", "R", x="1")
        self.insert("mid", "M", root_id="R")
        self.insert("leaf", "L", mid_id="M", mid2_id="M")
        result = self.delete("root", "R")
        got = {(x["table"], x["row_id"]) for x in result["cascade_deletes"]}
        self.assertEqual(got, {("mid", "M"), ("leaf", "L")})
        self.assertEqual(self.ids("root") | self.ids("mid") | self.ids("leaf"),
                         set())


# ---------------------------------------------------------------------------
# Registration concurrency and old transactions
# ---------------------------------------------------------------------------


class TestRegistrationConcurrency(RefCase):
    def test_old_read_only_transaction_keeps_its_historical_view(self):
        self.table("late", "v")
        self.table("latech", "rid")
        self.insert("late", "L", v="1")
        reader = self.c.begin()
        self.assertEqual([r["row_id"] for r in reader.read("late")], ["L"])
        # A new constraint is registered while the reader stays open.
        self.c.register_reference("later", "late", "latech", "rid", "cascade")
        # Its reads still complete and it serializes at its start view.
        self.assertEqual([r["row_id"] for r in reader.read("late")], ["L"])
        self.assertEqual(reader.commit()["status"], "committed")

    def test_write_transaction_started_before_registration_is_checked(self):
        self.table("late", "v")
        self.table("latech", "rid")
        writer = self.c.begin()
        writer.stage_insert("latech", "x", {"rid": "missing"})
        self.c.register_reference("later", "late", "latech", "rid", "cascade")
        with self.assertRaises(ConflictError) as info:
            writer.commit()
        self.assertEqual(info.exception.code, "reference_violation")
        self.assertEqual(self.ids("latech"), set())

    def test_two_disjoint_parent_child_groups_both_commit(self):
        self.table("p", "v")
        self.table("ch", "p_id")
        self.c.register_reference("rc", "p", "ch", "p_id", "cascade")
        t1 = self.c.begin()
        t2 = self.c.begin()
        t1.stage_insert("p", "G1", {"v": "1"})
        t1.stage_insert("ch", "G1-c", {"p_id": "G1"})
        t2.stage_insert("p", "G2", {"v": "2"})
        t2.stage_insert("ch", "G2-c", {"p_id": "G2"})
        self.assertEqual(t1.commit()["status"], "committed")
        self.assertEqual(t2.commit()["status"], "committed")
        self.assertEqual(self.ids("ch"), {"G1-c", "G2-c"})

    def test_cascade_conflict_evidence_names_concurrent_writer(self):
        self.table("p", "v")
        self.table("ch", "p_id", "note")
        self.c.register_reference("rc", "p", "ch", "p_id", "cascade")
        self.insert("p", "P", v="1")
        self.insert("ch", "C", p_id="P", note="old")
        deleter = self.c.begin()
        deleter.stage_delete("p", "P")
        updater = self.c.begin()
        updater.stage_update("ch", "C", {"note": "new"})
        self.assertEqual(updater.commit()["status"], "committed")
        with self.assertRaises(ConflictError) as info:
            deleter.commit()
        self.assertEqual(info.exception.code, "cascade_conflict")
        problem = info.exception.evidence["conflicts"][0]
        self.assertEqual(problem["table"], "ch")
        self.assertEqual(problem["row_id"], "C")
        self.assertEqual(problem["other_transaction"], updater.txid)
        # Parent and child both survive: no orphan, no silent delete.
        self.assertIn("P", self.ids("p"))
        self.assertIn("C", self.ids("ch"))
        self.assertEqual(self.c.rows("ch")[0]["note"], "new")


class TestSubscriptionIntegration(RefCase):
    def test_cross_table_cascade_is_one_batch_with_single_position(self):
        from mrtx import Replica
        self.table("p", "v")
        self.table("ch", "p_id")
        self.c.register_reference("rc", "p", "ch", "p_id", "cascade")
        self.insert("p", "P", v="1")
        self.insert("ch", "C", p_id="P")
        sub, _cut = self.c.create_subscription("s", ["p", "ch"])
        replica_path = os.path.join(self.rr.dir, "replica.db")
        rep = Replica(replica_path, sub.handle)
        rep.run(self.c)
        outcome = self.delete("p", "P")
        position = outcome["commit_version"]
        # The whole net change (explicit parent delete plus implicit child
        # delete) is delivered as one incremental batch.
        rep.run(self.c)
        self.assertEqual(rep.state()["position"], position)
        self.assertEqual(rep.rows("p"), [])
        self.assertEqual(rep.rows("ch"), [])
        # Source and replica agree on the row history versions.
        history_versions = [h["version"] for h in self.c.history("ch", "C")]
        self.assertEqual(history_versions[-1], position)
        self.assertEqual(len(history_versions), 2)


if __name__ == "__main__":
    unittest.main()
