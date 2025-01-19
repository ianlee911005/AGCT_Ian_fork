import os

from .model import VEAnalysisResult
from .file_util import (
    unique_file_name,
    create_folder
)


class VEAnalysisExporter:

    def export_results(self, results: VEAnalysisResult,
                       dir: str):
        dir = unique_file_name(dir, "ve_analysis_data_")
        create_folder(dir)
        results.general_metrics.to_csv(
            os.path.join(dir, "general_metrics.csv"), index=False)
        results.roc_metrics.to_csv(
            os.path.join(dir, "roc_metrics.csv"), index=False)
        results.roc_curve_coordinates.to_csv(
            os.path.join(dir, "roc_curve_coords.csv"), index=False)

