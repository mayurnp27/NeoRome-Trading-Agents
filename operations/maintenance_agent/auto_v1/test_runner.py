"""Trusted isolated runner; zero discovered tests is a failure."""
import importlib,inspect,os,sys,tempfile,unittest
from pathlib import Path

if __name__=='__main__':
    sys.path.insert(0,os.getcwd())
    kind,*modules=sys.argv[1:]
    if kind=='unittest':
        suite=unittest.defaultTestLoader.loadTestsFromNames(modules)
        if suite.countTestCases()<1:raise SystemExit('No tests discovered')
        raise SystemExit(not unittest.TextTestRunner(verbosity=1).run(suite).wasSuccessful())
    if kind=='functions':
        # These admitted suites use ordinary assert functions and the sole
        # tmp_path fixture. Unsupported fixture semantics fail closed.
        suite=unittest.TestSuite()
        for name in modules:
            module=importlib.import_module(name)
            for key,fn in sorted(vars(module).items()):
                if key.startswith('test_') and inspect.isfunction(fn):
                    args=list(inspect.signature(fn).parameters)
                    if args not in ([],['tmp_path']):raise SystemExit('Unsupported fixture')
                    def run(fn=fn,args=args):
                        with tempfile.TemporaryDirectory() as tmp:fn(**({'tmp_path':Path(tmp).resolve()} if args else {}))
                    suite.addTest(unittest.FunctionTestCase(run,description=name+'.'+key))
        if not suite.countTestCases():raise SystemExit('No tests discovered')
        raise SystemExit(not unittest.TextTestRunner(verbosity=1).run(suite).wasSuccessful())
    raise SystemExit('Unknown trusted test runner')
