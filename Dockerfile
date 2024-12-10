# Use a mageai base image
FROM mageai/mageai

# Set working directory
RUN pip install numpy pandas scikit-learn influxdb-client matplotlib seaborn statsmodels plotly joblib