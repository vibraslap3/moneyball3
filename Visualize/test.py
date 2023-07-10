import matplotlib.pyplot as plt
import seaborn as sns   # Importing seaborn for sample data and heatmap style

# Load sample data
flights = sns.load_dataset("flights")
data = flights.pivot("month", "year", "passengers")

# Set the figure size
plt.figure(figsize=(10, 6))  # Adjust the width and height as per your requirement

# Create the heatmap
sns.heatmap(data, annot=True, fmt="d", linewidths=.5, cmap="YlGnBu")

# Display the heatmap
plt.show()
