import dash
import dash_core_components as dcc
import dash_html_components as html
import dash_table_experiments as dt
from dash.dependencies import Input, Output, State
import pandas as pd
import numpy as np
import os
import plotly.graph_objs as go
import plotly.figure_factory as ff
from ..api import utils
import json

class App:
	def __init__(self, mhealthObject):
		self._m = mhealthObject
		self._root_folder = os.path.normpath(os.path.abspath(self._m.get_root()))
		self._summary_table = None
		self._subject_table = self._m.merged_subject_meta()
		self._participants = self._m.participants
		self._location_table = self._m.merged_location_mapping('Sensor_location_lab.csv')
		self._session_table = self._m.merged_session_meta()
		self._app = dash.Dash(name='mHealth Data Quality Check')
		self._app.config['suppress_callback_exceptions']=True

	def run(self):
		self._app.run_server(debug=True)

	def setup_callbacks(self):
		app = self._app
		@app.callback(Output('refresh-state', 'children'),[Input('refresh-check', 'n_clicks')])
		def refresh_state(n_clicks):
			if n_clicks == 0:
				summary_file = os.path.join(self._root_folder, 'DerivedCrossParticipants', 'quality_check_summary.csv')
				if os.path.exists(summary_file):
					self._summary_table = pd.read_csv(summary_file)
					message = 'Loaded'
				else:
					message = 'quality_check_summary.csv not found'
			else:
				self._summary_table = self._m.summarize(use_parallel=True, verbose=True)
				message = 'Refreshed'
			if self._summary_table is not None:
				self._median_num_of_rows = np.median(self._summary_table.loc[self._summary_table['type'] == 'sensor','num_of_rows'].values)
				device_counts = self._summary_table.groupby(['date', 'hour', 'type','pid'])['id'].count().reset_index()
				self._major_num_of_devices = utils.major_element(device_counts.loc[device_counts['type'] == 'sensor', 'id'].values)
				self._major_num_of_annotators = utils.major_element(device_counts.loc[device_counts['type'] == 'annotation', 'id'].values)
			return message

		# @app.callback(Output('summary-table-container', 'children'),[Input('refresh-state', 'children'), Input('cross-filter-participant', 'value')])
		# def load_summary_table(state, participant):
		# 	if state == 'Refreshed' or state == 'Loaded':
		# 		return self._create_summary_table(participant)
		# 	else:
		# 		return html.H3("Not ready")

		@app.callback(Output('quality-check-heatmap-container', 'children'),[Input('refresh-state', 'children'), Input('cross-filter-participant', 'value')])
		def load_quality_check_heatmap(state, participant):
			if state == 'Refreshed' or state == 'Loaded':
				return self._create_quality_check_heatmap(participant)
			else:
				return html.H3("Not ready")

		# @app.callback(Output('location-mapping-table-container', 'children'),[Input('refresh-state', 'children'), Input('cross-filter-participant', 'value')])
		# def load_location_mapping_table(state, participant):
		# 	if state == 'Refreshed' or state == 'Loaded':
		# 		return self._create_location_mapping_table(participant)
		# 	else:
		# 		return html.H3("Not ready")

		# @app.callback(Output('subject-meta-table-container', 'children'),[Input('refresh-state', 'children'), Input('cross-filter-participant', 'value')])
		# def load_subject_meta_table(state, participant):
		# 	if state == 'Refreshed' or state == 'Loaded':
		# 		return self._create_subject_meta_table(participant)
		# 	else:
		# 		return html.H3("Not ready")


		@app.callback(Output('save-quality-check-summary', 'disabled'),[Input('refresh-state', 'children')])
		def enable_save_button(children):
			if children == 'Refreshed':
				return False
			else:
				return True

		@app.callback(Output('save-state', 'children'), [Input('save-quality-check-summary', 'n_clicks')])
		def save_quality_check_summary(n_clicks):
			if n_clicks == 0:
				return "Refresh and then save"
			else:
				output_path = os.path.join(self._root_folder, 'DerivedCrossParticipants', 'quality_check_summary.csv')
				if not os.path.exists(os.path.dirname(output_path)):
					os.makedirs(os.path.dirname(output_path))
				self._summary_table.to_csv(output_path, index=False)
				return "Saved to quality_check_summary.csv"
		
		@app.callback(Output('quality-check-problems', 'children'),[Input('quality-check-heatmap', 'hoverData')])
		def show_quality_check_problems(hoverData):
			return hoverData['points'][0]['text']

	def _create_summary_table(self, participant):
		if self._summary_table is None:
			return html.H3(children='Cannot find quality_check_summary.csv in DerivedCrossParticipants folder')
		else:
			filtered_table = self._filter_summary_table(participant)
			return dt.DataTable(
				rows=filtered_table.to_dict("records"),
				columns=filtered_table.columns,
				row_selectable=True,
				filterable=True,
				sortable=True,
				id='table-summary'
			)

	def _filter_summary_table(self, participant, date=None, hour=None):
		print(participant)
		print(date)
		print(hour)
		filtered_table = self._summary_table.loc[self._summary_table['pid'] == participant,:]
		if date is not None:
			filtered_table = filtered_table.loc[self._summary_table['date'] == date,:]
		if hour is not None:
			filtered_table = filtered_table.loc[self._summary_table['hour'] == hour,:]
		return filtered_table

	def _quality_check_score(self, participant, date, hour):
		filtered_table = self._filter_summary_table(participant, date, hour)
		total_checks = 0
		incorrect_checks = 0
		message = ""
		# first check number of devices
		num_of_devices = len(np.unique(filtered_table.loc[filtered_table['type'] == 'sensor', 'id']))
		total_checks = total_checks + 1
		if num_of_devices != self._major_num_of_devices:
			incorrect_checks = incorrect_checks + 1
			message = message + "NUM_OF_DEVICES: " + str(num_of_devices) + "/" + str(self._major_num_of_devices) + "\n\n"

		# then check number of annotators
		num_of_annotators = len(np.unique(filtered_table.loc[filtered_table['type'] == 'annotation', 'id']))
		if num_of_annotators != self._major_num_of_annotators:
			incorrect_checks = incorrect_checks + 1
			message = message + "NUM_OF_ANNOTATORS: " + str(num_of_annotators) + "/" + str(self._major_num_of_annotators) + "\n\n"
		total_checks = total_checks + 1
		
		# loop over each file and check
		
		for index, row in filtered_table.iterrows():
			before_check = incorrect_checks
			file_message = ""
			# check mh_structure
			total_checks = total_checks + 1
			if row['mh_folder_structure'] != "True" and row['mh_folder_structure'] != True:
				incorrect_checks = incorrect_checks + 1
				file_message = file_message + "FOLDER_STRUCTURE: False\n\n"
			# check mh_filename
			total_checks = total_checks + 1
			if row['mh_filename'] != "True" and row['mh_filename'] != True:
				incorrect_checks = incorrect_checks + 1
				file_message = file_message + "FILENAME_PATTERN: False\n\n"
			# check csv header
			total_checks = total_checks + 1
			if row['csv_header'] != "True" and row['csv_header'] != True:
				incorrect_checks = incorrect_checks + 1
				file_message = file_message + "CSV_HEADER_ERROR: " + str(row['csv_header']) + "\n\n"
			# check NA rows
			total_checks = total_checks + 1
			if row['na_rows'] > 0:
				incorrect_checks = incorrect_checks + 1
				file_message = file_message + "NA_ROWS: " + str(row['na_rows']) + "\n\n"
			# check number of rows
			total_checks = total_checks + 1
			if row['type'] == 'sensor':
				major_num_of_rows = utils.major_element(filtered_table.loc[filtered_table['sensortype'] == row['sensortype'], 'num_of_rows'].values)
				if row['num_of_rows'] < major_num_of_rows * 0.9:
					incorrect_checks = incorrect_checks + 1
					file_message = file_message + "NUM_OF_ROWS: " + str(row['num_of_rows']) + "/" + str(major_num_of_rows) + "\n\n"
			elif row['type'] == 'annotation' or row['type'] == 'event':
				if row['num_of_rows'] < 1:
					incorrect_checks = incorrect_checks + 1
					file_message = file_message + "NUM_OF_ROWS: " + str(row['num_of_rows']) + "\n\n"
			if incorrect_checks - before_check > 0:
				file_message = "" + str(row['id']) + ", " + str(row['datatype']) + ', ' + str(row['sensortype']) + ", " + str(row['type']) + ":" + "\n\n" + file_message
				message = message + file_message
		score = incorrect_checks / total_checks
		return (score, message) # higher the worse
					
	def _create_quality_check_heatmap(self, participant):
		if self._summary_table is None:
			return html.H3(children='Summary data is not available')
		else:
			filtered_table = self._filter_summary_table(participant)
			# generate date-hour heatmap
			datehours = filtered_table[['date', 'hour']].drop_duplicates().sort_values(by=['date', 'hour'])
			first_datehour = datehours.values[0, 0] + '-' + str(datehours.values[0,1])
			last_datehour = datehours.values[datehours.shape[0]-1,0] + '-' + str(datehours.values[datehours.shape[0]-1,1])
			all_datehours = pd.date_range(start=first_datehour, end=last_datehour, freq='1H')
			y_hours = np.unique(all_datehours.map(lambda x: x.hour).values).tolist()
			x_dates = np.unique(all_datehours.map(lambda x: x.strftime('%Y-%m-%d')).values).tolist()
			z_data = []
			text_data = []
			for hour in y_hours:
				z_row=[]
				text_row = []
				for date in x_dates:
					score, message = self._quality_check_score(participant, date, hour)
					z_row.append(score)
					text_row.append(message)
				z_data.append(z_row)
				text_data.append(text_row)

			figure = ff.create_annotated_heatmap(z_data, x=x_dates, y=y_hours, colorscale='RdBu', zmin=0, zmax=1, showscale=True, text=text_data, hoverinfo='z', xgap=2, ygap=2)
			
			layout = go.Layout(
				title = 'Quality Check Heatmap for ' + str(participant),
				xaxis = dict(title='Date', tickformat='%Y-%m-%d'),
				yaxis = dict(title='Hour'),
				
			)
			figure.layout = layout
			return dcc.Graph(id='quality-check-heatmap', figure=figure)

	def _create_participant_dropdown(self):
		options = [{'label': p, 'value': p} for p in self._participants]
		return html.Div(children=dcc.Dropdown(
			options=options,
			value=self._participants[0],
			id='cross-filter-participant'
		), style={'width': '20%'})

	def _create_location_mapping_table(self, participant):
		if self._location_table.shape[0] == 0:
			return "Cannot find any sensor location mapping files"
		else:
			filtered_table = self._location_table.loc[self._location_table['PID'] == participant,:]
			return dt.DataTable(
				rows=filtered_table.to_dict("records"),
				columns=filtered_table.columns,
				row_selectable=True,
				filterable=True,
				sortable=True,
				id='table-location'
			)
	
	# def _create_subject_meta_table(self, participant):
	# 	if self._subject_table.shape[0] == 0:
	# 		return "Cannot find any subject meta files"
	# 	else:
	# 		filtered_table = self._subject_table.loc[self._subject_table['SUBJECT_ID'] == participant,:]
	# 		return dt.DataTable(
	# 			rows=filtered_table.to_dict("records"),
	# 			columns=filtered_table.columns,
	# 			row_selectable=True,
	# 			filterable=True,
	# 			sortable=True,
	# 			id='table-subject'
	# 		)

	def init_layout(self):
		self._app.layout = html.Div(children=[
			html.H1(children='mhealth Data Quality Check'),
			html.P(children=self._root_folder),
			html.Button(id='refresh-check', n_clicks=0, children='Run quality check'),
			html.Button(id='save-quality-check-summary', n_clicks=0, children='Save quality check', disabled='true'),
			html.P(id='refresh-state'),
			html.P(id='save-state'),
			self._create_participant_dropdown(),
			html.H2(children='Meta File Check'),
			html.Div(id='meta-file-div', children=[
				html.Div(id='location-mapping-div', children=[
					html.H3(children='Location Mapping Table'),
				html.Div(id='location-mapping-table-container'),
				], style={'float': 'left', 'width': '33.3%'}),
				# html.Div(id='subject-meta-div', children=[
				# 	html.H3(children='Subject Meta Table'),
				# html.Div(id='subject-meta-table-container'),
				# ], style={'float': 'left', 'width': '33.3%'})
			]),
			html.H2(children='Data Quality Heatmap'),
			html.Div(id='quality-check-heatmap-container', style={'float': 'left', 'width': '50%'}),
			dcc.Markdown(id='quality-check-problems', containerProps=dict(style={'float': 'left', 'width': '50%', 'height': 400, 'overflow': 'auto'})),
			html.Div(id='summary-table-container', style={'clear': 'both'}),
			
			# html.H2(children='Session Table'),
			# dt.DataTable(
			# 	rows=self._session_table.to_dict("records"),
			# 	columns=self._session_table.columns,
			# 	row_selectable=True,
			# 	filterable=True,
			# 	sortable=True,
			# 	id='table-session'
			# ),
		])