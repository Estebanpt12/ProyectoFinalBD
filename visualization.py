# visualization.py

import matplotlib.pyplot as plt

def plot_top_words(df):
    """Genera una gráfica de barras con las palabras más frecuentes."""
    top_words_df = df.toPandas()
    plt.figure(figsize=(12, 8))
    plt.barh(top_words_df["word"], top_words_df["count"], color="skyblue")
    plt.xlabel("Frequency")
    plt.ylabel("Word")
    plt.title("Top Words Frequency")
    plt.gca().invert_yaxis()
    plt.show()