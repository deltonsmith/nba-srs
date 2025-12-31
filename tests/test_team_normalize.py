import unittest

from src.team_normalize import CANONICAL_TEAMS, NBA_ID_TO_ABBR, normalize_team_id, validate_canonical_teams


class TestTeamNormalize(unittest.TestCase):
    def test_canonical_integrity(self):
        validate_canonical_teams()
        self.assertEqual(len(CANONICAL_TEAMS), 30)
        self.assertEqual(len(set(CANONICAL_TEAMS.keys())), 30)
        self.assertEqual(len(set(NBA_ID_TO_ABBR.keys())), 30)

    def test_common_variants(self):
        self.assertEqual(normalize_team_id("Los Angeles Lakers"), "LAL")
        self.assertEqual(normalize_team_id("LA Lakers"), "LAL")
        self.assertEqual(normalize_team_id("Los Angeles Clippers"), "LAC")
        self.assertEqual(normalize_team_id("LA Clippers"), "LAC")
        self.assertEqual(normalize_team_id("Golden State Warriors"), "GSW")
        self.assertEqual(normalize_team_id("New York Knicks"), "NYK")
        self.assertEqual(normalize_team_id("Brooklyn Nets"), "BKN")

    def test_numeric_ids(self):
        self.assertEqual(normalize_team_id(14), "LAL")
        self.assertEqual(normalize_team_id("14"), "LAL")
        self.assertEqual(normalize_team_id(21), "OKC")


if __name__ == "__main__":
    unittest.main()
