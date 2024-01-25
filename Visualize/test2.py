import plotly.graph_objects as go

# Create the figure
fig = go.Figure()

# Add the bars to the figure
fig.add_bar(x=["Category 1", "Category 2", "Category 3"],
            y=[10, 20, 15],
            text=["Label 1", "Label 2", "Label 3"],
            textangle=270)

# Update the layout
fig.update_layout(title="Bar Chart with Rotated Labels")

# Show the figure
fig.show()