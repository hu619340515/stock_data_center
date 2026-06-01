import unittest
from unittest.mock import Mock, patch

import viewer.server as server


class TestViewerRoutes(unittest.TestCase):
    def setUp(self):
        server.app.config["TESTING"] = True
        self.client = server.app.test_client()

    def test_weekly_route_runs_stock_then_etf(self):
        with patch.object(server, "_run_task_sequence") as run_sequence:
            run_sequence.return_value = (True, "ok")

            response = self.client.post("/api/aggregate_weekly", json={})

            self.assertEqual(response.status_code, 200)
            steps = run_sequence.call_args.args[0]
            self.assertEqual([step["asset_type"] for step in steps], ["stock", "etf"])
            stock_pipeline = Mock()
            etf_pipeline = Mock()
            steps[0]["func"](stock_pipeline)
            steps[1]["func"](etf_pipeline)
            stock_pipeline.full_download_pipeline.assert_called_once_with("w")
            etf_pipeline.etf_download_pipeline.assert_called_once_with("w")

    def test_monthly_route_runs_stock_then_etf(self):
        with patch.object(server, "_run_task_sequence") as run_sequence:
            run_sequence.return_value = (True, "ok")

            response = self.client.post("/api/aggregate_monthly", json={})

            self.assertEqual(response.status_code, 200)
            steps = run_sequence.call_args.args[0]
            self.assertEqual([step["asset_type"] for step in steps], ["stock", "etf"])
            stock_pipeline = Mock()
            etf_pipeline = Mock()
            steps[0]["func"](stock_pipeline)
            steps[1]["func"](etf_pipeline)
            stock_pipeline.full_download_pipeline.assert_called_once_with("m")
            etf_pipeline.etf_download_pipeline.assert_called_once_with("m")

    def test_rps_route_starts_background_calculation(self):
        with patch.object(server, "_run_rps_task") as run_rps_task:
            run_rps_task.return_value = (True, "ok")

            response = self.client.post("/api/calculate_rps", json={})

            self.assertEqual(response.status_code, 200)
            run_rps_task.assert_called_once_with()


if __name__ == "__main__":
    unittest.main()
