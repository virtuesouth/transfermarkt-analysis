# ⚽ Transfermarkt Data Analysis with PySpark

This project explores football player transfer data from [Transfermarkt](https://www.kaggle.com/datasets/davidcariboo/player-scores) using the **Bronze–Silver–Gold architecture** with **PySpark** for scalable data processing and **Matplotlib/Seaborn** for visual insights.

The goal was to uncover patterns in player transfers, evaluate positional trends, and explore club spending vs. success dynamics over the last 10 years.

---

## 🗂 Project Structure 
- **📁 data/**  
  Raw CSVs: players.csv, transfers.csv  

- **📁 notebooks/**  
  Bronze, Silver, and Gold processing scripts (PySpark)

- **📁 figures/**  
  All visualizations generated in the analysis

  ## 🔍 Dataset Source
- 📦 **Source**: [Kaggle – Transfermarkt Data]([https://www.kaggle.com/datasets/josephvm/transfermarkt-data](https://www.kaggle.com/datasets/davidcariboo/player-scores))
- 🧮 **Size**: 60K+ transfers, 25K+ players
- 🕒 **Time Frame**: Filtered to last 10 years (2015–2025)

---

## ⚙️ Technologies Used
- PySpark for distributed data processing
- Pandas for local aggregations
- Matplotlib & Seaborn for plotting
- Git + GitHub for version control
- Project structure inspired by Medallion (Bronze/Silver/Gold)
