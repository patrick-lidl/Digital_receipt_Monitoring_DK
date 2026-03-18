import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from .kpi_monitoring_data import KpiMonitoringData


class KpiMonitoringPlotter:
    """Bundled plotting functions for KpiMonitoringData."""

    def __init__(self, data: KpiMonitoringData) -> None:
        """Copy the KpiMonitoringData instance."""
        self._data = data

    def plot_anomalies_per_kpi(self) -> go.Figure:
        """Create line plots of anomalies per week."""
        anomalies_per_kpi = self._data.get_anomalies_per_week(per_kpi=True)
        anomalies_per_kpi.rename(columns={"count": "ANZAHL"}, inplace=True)

        anomalies_total = self._data.get_anomalies_per_week(per_kpi=False)
        anomalies_total["KENNZAHL"] = "# Auffälligkeiten"
        anomalies_total.rename(columns={"count": "ANZAHL"}, inplace=True)

        bons_total = self._data.get_bons_with_anomalies_per_week()
        bons_total["KENNZAHL"] = "# Bons mit Auffälligkeiten"
        bons_total.rename(columns={"count": "ANZAHL"}, inplace=True)

        # Combine all data for plotting
        plot_df = pd.concat([anomalies_per_kpi, anomalies_total, bons_total], ignore_index=True)
        plot_df["KENNZAHL"] = plot_df["KENNZAHL"].str.split(":").str[0]

        # Create the line plot
        fig = px.line(
            plot_df,
            x="WOCHE",
            y="ANZAHL",
            color="KENNZAHL",
            title="Anzahl Auffälligkeiten pro Woche",
        )
        fig.update_layout(xaxis_type="category", xaxis_title="Woche", yaxis_title="Anzahl")
        return fig

    def plot_flag_statistic_per_week(self, statistic_name: str) -> go.Figure:
        """Create line plots for each KPI, for a given statistic."""
        input_df = self._data.get_weekly_flag_stats()

        filtered_df = input_df[input_df["Metrik"] == statistic_name].drop(columns=["Metrik"])
        melted_df = filtered_df.melt(id_vars=["WOCHE"], var_name="KPI", value_name="Value")

        # Shorten KPI name, if it uses a colon
        melted_df["KPI"] = melted_df["KPI"].str.split(":").str[0]

        fig = px.line(
            melted_df,
            x="WOCHE",
            y="Value",
            color="KPI",
            title=f"Wochentrend für: '{statistic_name}'",
        )
        fig.update_layout(xaxis_title="Woche", yaxis_title="Wert", xaxis_type="category")
        return fig

    def plot_flag_distribution(self, only_last_week: bool = False) -> go.Figure:
        """Show the distributions of flags over the stores and RLs."""
        fig = make_subplots(
            rows=2,
            cols=3,
            vertical_spacing=0.15,
            horizontal_spacing=0.05,
            subplot_titles=("Rote Flags (FLAG > 1)", "Orange Flags (FLAG = 1)", "Alle Flags"),
        )

        for row, agg_level in enumerate(["REGIONALLEITER", "FILIALNR"], start=1):
            all_flag_counts = self._data.get_flag_counts(agg_level)

            if only_last_week:
                last_week = self._data.weeks_in_data[-1]
                all_flag_counts = all_flag_counts[all_flag_counts["WOCHE"] == last_week]

            fig.add_trace(
                go.Histogram(x=all_flag_counts["count_rot"], name=f"{agg_level} Rot"),
                row=row,
                col=1,
            )
            fig.add_trace(
                go.Histogram(x=all_flag_counts["count_orange"], name=f"{agg_level} Orange"),
                row=row,
                col=2,
            )
            fig.add_trace(
                go.Histogram(x=all_flag_counts["count_alle"], name=f"{agg_level} Alle"),
                row=row,
                col=3,
            )

            y_title = "Anzahl RLs" if agg_level == "REGIONALLEITER" else "Anzahl Filialen"
            fig.update_yaxes(title_text=y_title, row=row, col=1)

        fig.update_layout(
            title_text="Verteilung der Flags pro Woche",
            showlegend=False,
            bargap=0.1,
        )
        # Update x-axis titles for the bottom row
        fig.update_xaxes(title_text="Anzahl Rote Flags", row=2, col=1)
        fig.update_xaxes(title_text="Anzahl Orange Flags", row=2, col=2)
        fig.update_xaxes(title_text="Anzahl Flags Gesamt", row=2, col=3)
        return fig

    def plot_flag_distribution_per_statistic(self, statistic_name: str) -> go.Figure:
        """Create box plots showing the distribution of a statistic over all stores."""
        input_df = self._data.get_per_store_flag_stats()
        plot_df = input_df[input_df["Metrik"] == statistic_name]

        fig = px.box(
            plot_df,
            x="KENNZAHL",
            y="Value",
            title=f"Verteilung über Filialen für: '{statistic_name}'",
        )
        fig.update_layout(xaxis_title="KPI", yaxis_title="Wert")
        return fig

    def boxplot_anomalies_per_store(self) -> go.Figure:
        """Create boxplots for each week of the anomalies per store."""
        fig = px.box(
            self._data.get_anomalies_per_store(),
            x="WOCHE",
            y="ANZAHL",
            title="Verteilung der Auffälligkeiten pro Filiale",
        )
        fig.update_layout(xaxis_type="category", xaxis_title="Woche", yaxis_title="Anzahl Auffälligkeiten")
        return fig
