from typing import Dict, List

import arakawa as ar
import pandas as pd
import plotly.graph_objects as go

from ch_utils.reporting.report_generator_base import ReportGeneratorBase

from .kpi_monitoring_data import KpiMonitoringData
from .kpi_monitoring_plotter import KpiMonitoringPlotter

FLAGGING_KPIS = [
    "Anzahl Bediener {color} | Landesweit",
    "Anzahl Bediener {color} | ⌀ RL",
    "Anzahl Bediener {color} | ⌀ Filiale",
    "Anzahl Bediener {color} | Median RL",
    "Anzahl Bediener {color} | Max RL",
    "Anzahl Bediener {color} | Min RL",
    "Anzahl Bediener {color} | Median Filiale",
    "Anzahl Bediener {color} | Max Filiale",
    "Anzahl Bediener {color} | Min Filiale",
]


class ReportGenerator(ReportGeneratorBase):
    """
    A class to generate structured HTML reports using the Arakawa library.

    Todo:
        - Add table for check_results_log of current run_id
        - Add warnings for stores / RLs with high number of red flags
    """

    def __init__(
        self,
        kpi_monitoring_data: KpiMonitoringData,
        title: str = "Report",
        sub_title: str = "<Placeholder>",
    ):
        """Initialize the ReportGenerator with a title."""
        super().__init__(title, sub_title)

        self._data = kpi_monitoring_data
        self._plotter = KpiMonitoringPlotter(kpi_monitoring_data)

        self._main_stats: List[ar.BigNumber] = []
        self._line_plots: Dict[str, go.Figure] = {}

    def add_overview_stats(self):
        """Add big stats to top of the report."""
        anomalies_per_week = self._data.get_anomalies_per_week(per_kpi=False)
        self._add_main_stat(anomalies_per_week["count"], "Anzahl Auffälligkeiten")

        bons_per_week = self._data.get_bons_with_anomalies_per_week()
        self._add_main_stat(bons_per_week["count"], "Anzahl auffällige Bons")

        # FIXME: these values are smaller than `self._data.get_flag_distribution_over_kpis()`
        flags_per_week = self._data.flags[["WOCHE", "FLAG"]].value_counts().sort_index().reset_index()
        self._add_main_stat(flags_per_week.loc[flags_per_week["FLAG"] == 2, "count"], "Anzahl Lupen")
        self._add_main_stat(flags_per_week.loc[flags_per_week["FLAG"] == 1, "count"], "Anzahl Fernglässer")

        self.sections.append(ar.Group(*self._main_stats, columns=len(self._main_stats)))

    def add_check_log(self, check_log: pd.DataFrame) -> None:
        """Add table for check_results_log of current run_id."""
        check_log_section = ar.Toggle(ar.DataTable(check_log), label="Check Log")
        self.sections.append(check_log_section)

    def add_anomaly_section(self) -> None:
        """Add a section analyzing the anomalies."""
        num_anomalies = len(self._data.anomalies[["BON-NR", "KENNZAHL"]].drop_duplicates())
        num_receipts_with_anomalies = len(self._data.anomalies["BON-NR"].unique())

        # FIXME: improve intro text
        text = [
            f"Anzahl der Auffälligkeiten landesweit: {num_anomalies}",
            f"Anzahl der auffälligen Bons landesweit: {num_receipts_with_anomalies}",
        ]

        new_section = ar.Toggle(
            ar.Blocks(ar.Text("<br>".join(text))),
            ar.Select(
                ar.Group(
                    ar.Plot(self._plotter.plot_anomalies_per_kpi()),
                    label="Auffälligkeiten pro KPI pro Woche",
                ),
                ar.Group(
                    ar.Plot(self._plotter.boxplot_anomalies_per_store()),
                    label="Verteilung über die Filialen pro Woche",
                ),
                ar.Group(
                    ar.Table(self._data.get_anomaly_overview_df()),
                    label="Auffälligkeiten pro KPI gesamt",
                ),
            ),
            label="Statistiken der Auffälligkeiten",
        )
        self.sections.append(new_section)

    def add_flag_sections(self) -> None:
        """Add a sections for flagging cashiers."""
        self.sections.append(
            ar.Toggle(
                ar.Select(
                    self._get_flags_per_rl_and_store(),
                    self._get_flags_per_kpi_overview(),
                    self._get_red_flags_per_week_per_kpi(),
                    self._get_orange_flags_per_week_per_kpi(),
                ),
                label="Statisiken der Flags",
            )
        )

    # ----------------------------------------------------------------------------------
    # Sub-section definitions
    # ----------------------------------------------------------------------------------

    def _get_flags_per_rl_and_store(self):
        return ar.Group(
            ar.Select(
                ar.Group(ar.Plot(self._plotter.plot_flag_distribution(only_last_week=True)), label="Letzte Woche"),
                ar.Group(ar.Plot(self._plotter.plot_flag_distribution()), label="Gesamt"),
            ),
            label="Verteilung der Flags pro Filiale & RL",
        )

    def _get_flags_per_kpi_overview(self):
        return ar.Group(
            ar.Table(self._data.get_flag_distribution_over_kpis().astype(int)), label="Verteilung der Flags pro KPI"
        )

    def _get_red_flags_per_week_per_kpi(self):
        return ar.Group(
            ar.Select(
                *[
                    ar.Group(
                        ar.Plot(self._plotter.plot_flag_statistic_per_week(label.format(color="rot"))),
                        label=label.format(color="rot").split(" | ")[-1],
                    )
                    for label in FLAGGING_KPIS
                ],
            ),
            label="Rote Flags pro KW",
        )

    def _get_orange_flags_per_week_per_kpi(self):
        return ar.Group(
            ar.Select(
                *[
                    ar.Group(
                        ar.Plot(self._plotter.plot_flag_statistic_per_week(label.format(color="orange"))),
                        label=label.format(color="orange").split(" | ")[-1],
                    )
                    for label in FLAGGING_KPIS
                ],
            ),
            label="Orange Flags pro KW",
        )

    # ----------------------------------------------------------------------------------
    # Internal Helper Methods
    # ----------------------------------------------------------------------------------

    def _add_main_stat(self, arr: pd.Series, heading: str):
        change = arr.iloc[-1] - arr.iloc[-2]
        self._main_stats.append(
            ar.BigNumber(
                heading=heading,
                value=arr.iloc[-1],
                change=abs(change),
                is_upward_change=change >= 0,
                is_positive_intent=change <= 0,
            )
        )
