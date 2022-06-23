sns.set_context('notebook')

data = session_streaks.iloc[:,:-1].reset_index(level='player_id', drop=True)/60

plt.figure(figsize=(12,12))
sns.heatmap(data.head(40).round(1), robust=True, annot=True, cmap="YlGnBu", square=False, linewidths=0.5)
plt.xticks(rotation=45)
plt.title("Hours Spent in Garden per Week per User (top 25 lifetime hours)")