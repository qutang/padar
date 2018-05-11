
import altair as alt
def view_annotation_summary(df):
	if 'pid' in df.columns:
		pids = df['pid'].unique()
		charts = []
		for pid in pids:
			charts.append(alt.Chart(df[df['pid'] == pid], title=pid, width=1200, height=800).mark_bar().encode(
				x=alt.X('DURATION_IN_SECONDS:Q', axis=alt.Axis(title='Duration (seconds)')),
				y=alt.Y('LABEL_NAME:N', axis=alt.Axis(title='Annotations'))
				)
			)
		chart = alt.vconcat(*charts).configure_axis(labelFontSize=14)
	else:
		if 'annotator' in df.columns:
			chart = alt.Chart(df, width=1200, height=800).mark_bar().encode(
				x=alt.X('DURATION_IN_SECONDS:Q', axis=alt.Axis(title='Duration (seconds)')),
				column=alt.Column('LABEL_NAME:N', axis=alt.Axis(title='Annotations')),
				y=alt.Y('annotator:N', axis=alt.Axis(title='Annotator')),
				color='annotator:N'
				)
	return chart

def view_sensor_summary(df):
	if 'pid' in df.columns:
		pids = df['pid'].unique()
		charts=[]
		for pid in pids:
			if 'sid' in df.columns:
				if 'location' in df.columns:
					df['sid_location'] = df['sid'].astype(str) + "_"  + df['location']
					color_col = 'sid_location'
				else:
					color_col = 'sid'
				charts.append(alt.Chart(df[df['pid'] == pid], title=pid, width=1200, height=800).mark_line().encode(
					y=df.columns[1],
					x=alt.X(df.columns[0], timeUnit='yearmonthdatehoursminutesseconds', type='temporal'),
					color=color_col
					).interactive(bind_y=False)
				)
			else:
				charts.append(alt.Chart(df[df['pid'] == pid], title=pid, width=1200, height=800).mark_line().encode(
					y=df.columns[1],
					x=alt.X(df.columns[0], timeUnit='yearmonthdatehoursminutesseconds', type='temporal')
					).interactive(bind_y=False)
				)
		chart = alt.vconcat(*charts).configure_axis(labelFontSize=14)
	else:
		if 'sid' in df.columns:
			if 'location' in df.columns:
				df['sid_location'] = df['sid'].astype(str) + "_"  + df['location']
				color_col = 'sid_location'
			else:
				color_col = 'sid'
			chart = alt.Chart(df, width=1200, height=800).mark_line().encode(
				y=df.columns[1],
				x=alt.X(df.columns[0], timeUnit='yearmonthdatehoursminutesseconds', type='temporal'),
				color=color_col
				).interactive(bind_y=False)
		else:
			chart = alt.Chart(df, width=1200, height=800).mark_line().encode(
				y=df.columns[1],
				x=alt.X(df.columns[0], timeUnit='yearmonthdatehoursminutesseconds', type='temporal')
				).interactive(bind_y=False)
	return chart