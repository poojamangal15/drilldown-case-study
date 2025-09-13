import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# --- Input your final data from SQL Query 1 here ---
data = {
    'industry': ['Aerospace', 'Pharmaceuticals', 'Defense', 'Banking', 'Logistics', 'Artificial Intelligence'],
    'total_revenue': [430000, 250000, 220000, 180000, 146000, 143400]
}
df = pd.DataFrame(data)

# --- Plotting ---
sns.set_theme(style="whitegrid")
plt.figure(figsize=(10, 6))

# Create the horizontal bar plot
ax = sns.barplot(
    data=df,
    x='total_revenue',
    y='industry',
    palette='viridis',
    orient='h'
)

# Add titles and labels
ax.set_title('Total Revenue by Top 6 Industries', fontsize=16, fontweight='bold', pad=15)
ax.set_xlabel('Total Revenue', fontsize=12)
ax.set_ylabel('Industry', fontsize=12)

# Format the x-axis labels as currency
formatter = plt.FuncFormatter(lambda x, p: f'${x:,.0f}')
ax.xaxis.set_major_formatter(formatter)

# Add data labels to the end of the bars for clarity
for container in ax.containers:
    ax.bar_label(container, fmt='$%g', label_type='edge', padding=5, fontsize=10)

plt.tight_layout()
plt.show()