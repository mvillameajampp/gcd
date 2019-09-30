import os
import time
import shutil
import tempfile

from unittest import TestCase, main

from gcd.meka import rule


@rule
def rule1(out="out", in1="in1", ins=["in2", "in3"], arg=0):
    rule1.called = False
    yield [in1, *ins], [out]
    touch(out)
    rule1.called = True


@rule
def rule2(out="out"):
    rule2.called = False
    yield [], [out]
    touch(out)
    rule2.called = True


def touch(*paths):
    for path in paths:
        time.sleep(0.01)
        with open(path, "w") as f:
            f.write("*")


class TestRule(TestCase):
    def setUp(self):
        self.cwd = tempfile.mkdtemp()
        os.chdir(self.cwd)
        touch("in1", "in2", "in3")

    def tearDown(self):
        shutil.rmtree(self.cwd)

    def test_uptodate(self):
        rule1()  # Run to create out.
        rule1()  # Run again without changing anything.
        self.assertFalse(rule1.called)

    def test_mtime_changed(self):
        rule1()  # Run to create out.
        touch("out")
        rule1()  # Run again with mtime(out) changed.
        self.assertTrue(rule1.called)
        touch("in1")
        rule1()  # Run again with mtime(in1) changed.
        self.assertTrue(rule1.called)

    def test_arg_changed(self):
        rule1()  # Run to create out.
        rule1(arg=1)  # Run again with different arguments.
        self.assertTrue(rule1.called)
        rule1(arg=1)  # Run again with same arguments than last time.
        self.assertFalse(rule1.called)

    def test_rule_changed(self):
        rule1()  # Run to create out.
        rule2()  # Run different rule with same out.
        self.assertTrue(rule2.called)
        rule2()  # Run again the last rule.
        self.assertFalse(rule2.called)

    def test_in_not_exists(self):
        os.remove("in3")
        with self.assertRaises(FileNotFoundError):
            rule1()  # Run without in3.

    def test_memo_not_exists(self):
        touch("out")
        rule1()  # Run with up to date files but no memo file.
        self.assertTrue(rule1.called)
        touch("out2")
        rule1(out="out2")  # Run with up to date files but no memo entry.
        self.assertTrue(rule1.called)

    def test_out_not_exists(self):
        rule1()  # Run to create out.
        os.remove("out")
        rule1()  # Run again without out.
        self.assertTrue(rule1.called)

    def test_rule_with_no_ins(self):
        rule2()
        self.assertTrue(rule2.called)


if __name__ == "__main__":
    main()
