from pathlib import Path
import tempfile
import unittest

from neorome.control_plane_api import ControlPlaneView
from neorome.control_plane_mcp import dispatch, tool_definitions
from neorome.control_plane_store import ControlPlaneStore


class ControlPlaneMCPTests(unittest.TestCase):
    def test_lists_only_read_tools_and_returns_structured_state(self):
        names = {item["name"] for item in tool_definitions()}
        self.assertIn("neorome_firm_status", names)
        self.assertFalse(any(word in name for name in names
                             for word in ("execute_trade", "submit_order", "shell", "sql", "transfer", "withdraw")))
        with tempfile.TemporaryDirectory() as root:
            with ControlPlaneStore(Path(root) / "control.sqlite3") as store:
                view = ControlPlaneView(store)
                initialized = dispatch(view, {"jsonrpc": "2.0", "id": 1, "method": "initialize"})
                self.assertEqual(initialized["result"]["serverInfo"]["name"], "neorome-control")
                result = dispatch(view, {"jsonrpc": "2.0", "id": 2, "method": "tools/call",
                                         "params": {"name": "neorome_firm_status", "arguments": {}}})
                self.assertIn("structuredContent", result["result"])


if __name__ == "__main__":
    unittest.main()
