import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

# Load the benchmark data from a CSV file
data = pd.read_csv('benchmark_results.csv')

# Check the data structure
print(data.head())

# Function to plot grouped bar charts for parquet file size and Compression Ratio
def plot_grouped_bar_charts(subset, x, y1, y2, xlabels, y1label, y2label, title, filename):
    formats = subset['format'].unique()
    n_formats = len(formats)
    
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 14))

    bar_width = 0.15
    index = np.arange(len(subset[x].unique()))
    
    # Plot parquet file size
    for i, fmt in enumerate(formats):
        fmt_subset = subset[subset['format'] == fmt]
        ax1.bar(index + i * bar_width, fmt_subset[y1], bar_width, label=f'{fmt}')

    ax1.set_xlabel('NDV / Number of Lines')
    ax1.set_ylabel(y1label)
    ax1.set_xticks(index + bar_width * (n_formats - 1) / 2)
    ax1.set_xticklabels(xlabels)
    ax1.legend(loc='upper left')
    ax1.set_title(f'{y1label} for Different Formats')
    ax1.grid(True)

    # Plot Compression Ratio
    for i, fmt in enumerate(formats):
        fmt_subset = subset[subset['format'] == fmt]
        ax2.bar(index + i * bar_width, fmt_subset[y2], bar_width, label=f'{fmt}')

    ax2.set_xlabel('NDV / Number of Lines')
    ax2.set_ylabel(y2label)
    ax2.set_xticks(index + bar_width * (n_formats - 1) / 2)
    ax2.set_xticklabels(xlabels)
    ax2.legend(loc='upper left')
    ax2.set_title(f'{y2label} for Different Formats')
    ax2.grid(True)

    fig.suptitle(title)
    fig.tight_layout()
    plt.savefig(filename, format='png')
    plt.show()

# Combine ndv and num_of_lines into a single column for x-axis labels
data['ndv_num_of_lines'] = data['ndv'].astype(str) + " / " + data['num_of_lines'].astype(str)

# Plot grouped bar chart for parquet file size and Compression Ratio
plot_grouped_bar_charts(data, 'ndv_num_of_lines', 'parquet_file_size', 'compressed_rate', 
                        data['ndv_num_of_lines'].unique(), 'Parquet File Size', 'Compression Ratio', 
                        'Parquet File Size and Compression Ratio for Different Formats',
                        '../figures/grouped_bar_charts.png')
