import matplotlib.pyplot as plt
# import seaborn as sns
# import plotly.express as px
import pandas as pd
import os
from .model import (
    VEAnalysisResult
)
from .date_util import now_str_compact
from .file_util import (
    unique_file_name,
    create_folder
)


class VEAnalysisPlotter:

    def __init__(self, settings: dict):
        self._settings = settings
        self._sys_ves_colors = settings['sys_ves_colors']
        self._user_ves_color = settings['user_ves_color']
        self._line_width = settings['line_width']
        self._line_style = settings['line_style']
        self._roc_title = settings['roc_title']
        self._plot_file_dpi = settings['file_dpi']
        self._bbox_inches = settings['bbox_inches']
        self._max_num_curves_per_plot = settings['max_num_ves_curves_per_plot']
        self._x_axis_font_size = settings['x_axis_font_size']
        self._y_axis_font_size = settings['y_axis_font_size']
        self._label_size = settings['label_size']
        self._title_font_size = settings['title_font_size']
        self._legend_font_size = settings['legend_font_size']

    def _plot_roc_curves(self, aucs: pd.DataFrame,
                         curve_coords: pd.DataFrame,
                         batch_no: int, num_batches: int,
                         file_name: str = None):
        title = self._roc_title + (f" ({batch_no} of {num_batches})" if
                                   num_batches > 1 else "")
        plt.figure(figsize=(10, 8))
        for i, ve_auc in aucs.iterrows():
            ve_curve_coords = curve_coords[curve_coords['SCORE_SOURCE'] ==
                                           ve_auc['SCORE_SOURCE']].sort_values(
                'THRESHOLD', ascending=False)
            if ve_auc['SCORE_SOURCE'] == 'USER':
                line_color = self._user_ves_color
            else:
                line_color = self._sys_ves_colors[i]
            label = ve_auc['SOURCE_NAME'] + \
                ' (AUC=' + str(round(ve_auc['ROC_AUC'], 4)) + ')'
            plt.plot(
                ve_curve_coords['FALSE_POSITIVE_RATE'],
                ve_curve_coords['TRUE_POSITIVE_RATE'],
                label=label,
                color=line_color, lw=self._line_width,
                linestyle=self._line_style)

        plt.xlabel('False Positive Rate', fontsize=self._x_axis_font_size)
        plt.ylabel('True Positive Rate', fontsize=self._y_axis_font_size)
        plt.tick_params(axis='both', labelsize=self._label_size)
        plt.title(title, fontsize=self._title_font_size)
        legend = plt.legend(loc="lower right", fontsize=self._legend_font_size)
        for line in legend.get_lines():
            line.set_linewidth(self._settings['legend_line_width'])
        if file_name is not None:
            plt.savefig(file_name, dpi=self._settings['file_dpi'],
                        format='png',
                        bbox_inches=self._settings['bbox_inches'])
        plt.show()

    def _display_roc_table(self, results: VEAnalysisResult,
                           file_name: str = None):
        table_output = results.general_metrics.merge(
            results.roc_metrics, how="inner", suffixes=(None, "_y"),
            on='SCORE_SOURCE')[[
                'SOURCE_NAME', 'NUM_VARIANTS', 'NUM_POSITIVE_LABELS',
                'NUM_NEGATIVE_LABELS', 'ROC_AUC']]
        table_output = table_output.sort_values('ROC_AUC', ascending=False)
        style = table_output.style.hide().relabel_index(
             ['VEP', "Variant Total", "Positive Labels", "Negative Labels",
              "ROC AUC"],
             axis=1).set_caption("ROC")
        style.to_html(file_name)

    def plot_roc_results(self, results: VEAnalysisResult,
                         dir: str = None):
        num_curves_per_plot = self._max_num_curves_per_plot
        roc_metric_batches = []
        roc_metrics = results.roc_metrics.sort_values('ROC_AUC',
                                                      ascending=False)
        for idx in range(0, len(roc_metrics), num_curves_per_plot):
            batch = roc_metrics.iloc[idx:idx+num_curves_per_plot]
            roc_metric_batches.append(batch)
        roc_curves_file_name = None if dir is None else os.path.join(
            dir, "roc_curves_")
        num_batches = len(roc_metric_batches)
        for batch_no, batch in enumerate(roc_metric_batches):
            batch_file_name = None if roc_curves_file_name is None else \
                roc_curves_file_name + str(batch_no) + ".png"
            self._plot_roc_curves(batch, results.roc_curve_coordinates,
                                  batch_no, num_batches, batch_file_name)
        roc_table_file_name = None if dir is None else os.path.join(
            dir, "roc_table" + ".html")
        self._display_roc_table(results, roc_table_file_name)

    def plot_results(self, results: VEAnalysisResult,
                     dir: str = None):
        if dir is not None:
            dir = unique_file_name(dir, "ve_analysis_plots_")
            create_folder(dir)
        self.plot_roc_results(results, dir)

    def plot_roc_results1(self, results: VEAnalysisResult,
                          dir: str = None):
        """
        num_curves_per_plot = self._max_num_curves_per_plot
        roc_metric_batches = []
        if results.num_user_variants is not None:
            num_curves_per_plot -= 1
            roc_metrics_no_user = results.roc_metrics.query(
                "SCORE_SOURCE != 'USER'")
            roc_metrics_user = results.roc_metrics.query(
                "SCORE_SOURCE == 'USER'")
            roc_metrics_no_user = roc_metrics_no_user.sort_values('ROC_AUC',
                                                                  ascending=False)
            for idx in range(0,len(roc_metrics_no_user),num_curves_per_plot):
                batch = roc_metrics_no_user.iloc[idx:idx+num_curves_per_plot]
                batch = pd.concat([batch, roc_metrics_user]).sort_values(
                    'ROC_AUC', ascending=False)
                roc_metric_batches.append(batch)
        else:
            roc_metrics_no_user = results.roc_metrics.sort_values('ROC_AUC',
                                                                  ascending=False)
            for idx in range(0,len(roc_metrics_no_user),num_curves_per_plot):
                batch = roc_metrics_no_user.iloc[idx:idx+num_curves_per_plot]
                roc_metric_batches.append(batch)
        """
        pass
