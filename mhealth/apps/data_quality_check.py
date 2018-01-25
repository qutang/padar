import dash
import dash_core_components as dcc
import dash_html_components as html
import dash_table_experiments as dt
import os

class App:
	def __init__(self, mhealthObject):
		self._m = mhealthObject
		self._root_folder = os.path.normpath(os.path.abspath(self._m.get_root()))
		# self._summary_table = self._m.summarize(use_parallel=True, verbose=False)
		self._subject_table = self._m.merged_subject_meta()
		self._location_table = self._m.merged_location_mapping('Sensor_location_lab.csv')
		self._session_table = self._m.merged_session_meta()
		self._app = dash.Dash(name='mHealth Data Quality Check')

	def run(self):
		self._app.run_server(debug=True)

	def init_layout(self):
	
		self._app.layout = html.Div(children=[
			html.H1(children='mhealth Data Quality Check'),
			html.P(children=self._root_folder),
			# dt.DataTable(
			# 	rows=self._summary_table.to_dict("records"),
			# 	columns=sorted(self._summary_table.columns),
			# 	row_selectable=True,
			# 	filterable=True,
			# 	sortable=True,
			# 	id='table-summary'
			# ),
			html.H2(children='Subject Meta Table'),
			dt.DataTable(
				rows=self._subject_table.to_dict("records"),
				columns=self._subject_table.columns,
				row_selectable=True,
				filterable=True,
				sortable=True,
				id='table-subject'
			),
			html.H2(children='Location Mapping Table'),
			dt.DataTable(
				rows=self._location_table.to_dict("records"),
				columns=self._location_table.columns,
				row_selectable=True,
				filterable=True,
				sortable=True,
				id='table-location'
			),
			html.H2(children='Session Table'),
			dt.DataTable(
				rows=self._session_table.to_dict("records"),
				columns=self._session_table.columns,
				row_selectable=True,
				filterable=True,
				sortable=True,
				id='table-session'
			),
			dcc.Graph(
				id='example-graph',
				figure={
					'data': [
						{'x': [1, 2, 3], 'y': [4, 1, 2], 'type': 'bar', 'name': 'SF'},
						{'x': [1, 2, 3], 'y': [2, 4, 5], 'type': 'bar', 'name': u'Montréal'},
					],
					'layout': {
						'title': 'Dash Data Visualization'
					}
				}
			)
		])