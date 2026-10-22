import json,tempfile,unittest
from pathlib import Path
from repair_probe import read_status

class ProbeTests(unittest.TestCase):
    def test_reads_small_valid_status(self):
        with tempfile.TemporaryDirectory() as tmp:
            p=Path(tmp)/'source.json';p.write_text(json.dumps({'items':[1,2]}))
            self.assertEqual(read_status(p),{'items':[1,2]})
if __name__=='__main__':unittest.main()
