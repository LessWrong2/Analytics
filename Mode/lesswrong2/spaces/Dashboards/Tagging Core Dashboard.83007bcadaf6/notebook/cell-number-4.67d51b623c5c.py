sns.set_context('talk')
sns.set_style('whitegrid')
sns.catplot(data=df[df['tag_name'].isin(['Rationality', 'AI', 'Coronavirus', 'World Modeling', 'World Optimization', 'Community', 'Practical'])], kind='count', x='mode', col='tag_name', col_wrap=3, sharex=False, 
              order = ['Hidden', '-25', '-10', '0', '10', '25', 'Required'], 
              color='orange', height=4, aspect=1.5)