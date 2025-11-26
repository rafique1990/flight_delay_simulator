import pytest
import polars as pl
from unittest.mock import patch, MagicMock
from pathlib import Path
from flightrobustness.io.visualizer import plot_arrival_delay_distribution

def test_plot_arrival_delay_distribution_success(tmp_path):
    """Test that plotting runs without error and saves a file."""
    # Mock the DataFrame chain to avoid dependency issues (pyarrow/pandas plotting)
    mock_df = MagicMock()
    mock_col = MagicMock()
    mock_pandas_series = MagicMock()
    
    mock_df.__getitem__.return_value = mock_col
    mock_col.to_pandas.return_value = mock_pandas_series
    
    output_dir = tmp_path
    
    with patch("flightrobustness.io.visualizer.plt") as mock_plt:
        plot_arrival_delay_distribution(mock_df, output_dir, bins=5)
        
        mock_plt.figure.assert_called_once()
        mock_plt.title.assert_called_once_with("Distribution of Actual Arrival Times")
        mock_plt.savefig.assert_called_once()
        mock_plt.close.assert_called_once()

def test_plot_arrival_delay_distribution_exception(tmp_path, capsys):
    """Test that exceptions during plotting are caught and printed."""
    df = pl.DataFrame({"ActualTimeOfArrival": [600]})
    
    with patch("flightrobustness.io.visualizer.plt") as mock_plt:
        mock_plt.figure.side_effect = Exception("Mocked plotting error")
        
        plot_arrival_delay_distribution(df, tmp_path)
        
        captured = capsys.readouterr()
        assert "Plotting failed: Mocked plotting error" in captured.out
