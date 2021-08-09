""" 
A simple app to ingest data from your local computer
to an azure datalake.
"""
from pywebio.input import input, select, actions, input_group
from pywebio.output import put_text
from pywebio import start_server



SOURCES = [
   {'label': 'Source', 'value': 0},
   {'label': 'Local computer', 'value': 1},
]

DESTINATION = [
   {'label': 'Destination', 'value': 0},
   {'label': 'Azure datalake', 'value': 1},
]


def app():             
    GROUPS = [ select(options=SOURCES, name='sources'), 
              input(placeholder='Local folder name', name='directory'),
              select('------------------------------------------------', 
              options=DESTINATION, name='destination'),
              input(placeholder='Access token', name='token'),
              input(placeholder='Container name', name='container'),
              ]
    source_data = input_group("Ingest your data", GROUPS)
    put_text(source_data['sources'], source_data['directory'],source_data['destination'],
             source_data['token'], source_data['container']) 
    print(source_data)
    




if __name__ == '__main__':
    start_server(app, port=8080)
     