from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parents[1]
STATE = "/var/lib/neorome-equity-intraday"
UNITS = (
    ROOT / "deploy/recorder/neorome-equity-intraday-watchlist.service",
    ROOT / "deploy/recorder/neorome-equity-intraday-sip.service",
)
OPPORTUNITY = ROOT / "deploy/recorder/neorome-equity-intraday-opportunity.service"
OPPORTUNITY_TIMER = ROOT / "deploy/recorder/neorome-equity-intraday-opportunity.timer"


def fields(path: Path) -> dict[str, list[str]]:
    result: dict[str, list[str]] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        if "=" in line and not line.lstrip().startswith("#"):
            key, value = line.split("=", 1)
            result.setdefault(key, []).append(value)
    return result


class EquityIntradayServiceTemplateTests(unittest.TestCase):
    def test_writer_units_declare_the_shared_systemd_state_directory(self):
        for unit in UNITS:
            values = fields(unit)
            self.assertEqual(values.get("User"), ["neorome"])
            self.assertEqual(values.get("Group"), ["neorome"])
            self.assertEqual(values.get("StateDirectory"), ["neorome-equity-intraday"])
            self.assertEqual(values.get("StateDirectoryMode"), ["0700"])
            self.assertIn(STATE, values.get("ReadWritePaths", []))
            self.assertEqual(values.get("UMask"), ["0077"])

    def test_commands_use_systemd_managed_state(self):
        watchlist, sip = (fields(unit)["ExecStart"][0] for unit in UNITS)
        self.assertIn(f"--state-dir {STATE}", watchlist)
        self.assertIn(f"--archive-root {STATE}", sip)
        self.assertIn(f"--universe {STATE}/frozen-universe-current.json", sip)

    def test_forward_only_opportunity_unit_uses_the_shared_paper_lifecycle_paths(self):
        values = fields(OPPORTUNITY)
        self.assertEqual(values.get("User"), ["neorome"])
        self.assertEqual(values.get("StateDirectory"), ["neorome-equity-intraday neorome-equity-intraday-opportunity"])
        self.assertEqual(values.get("StateDirectoryMode"), ["0700"])
        self.assertTrue(any("/var/lib/neorome-paper" in value for value in values.get("ReadWritePaths", [])))
        command = values["ExecStart"][0]
        self.assertIn("-m neorome.equity_intraday_opportunity", command)
        self.assertIn(f"--archive-root {STATE}", command)
        self.assertIn(f"--universe {STATE}/frozen-universe-current.json", command)
        self.assertIn("--state-dir /var/lib/neorome-equity-intraday-opportunity", command)
        self.assertIn("--ledger /var/lib/neorome-paper/paper.sqlite3", command)
        self.assertEqual(values.get("UMask"), ["0077"])

    def test_forward_only_timer_covers_every_registered_entry_minute_without_catch_up(self):
        values = fields(OPPORTUNITY_TIMER)
        self.assertEqual(values.get("OnCalendar"), [
            "Mon..Fri *-*-* 09:46..59:00 America/New_York",
            "Mon..Fri *-*-* 10:00..59:00 America/New_York",
        ])
        self.assertEqual(values.get("Persistent"), ["false"])
        self.assertEqual(values.get("Unit"), ["neorome-equity-intraday-opportunity.service"])


if __name__ == "__main__":
    unittest.main()
