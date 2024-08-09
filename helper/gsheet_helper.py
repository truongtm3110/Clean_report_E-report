import os
import re

from googleapiclient.discovery import build
# from apiclient.discovery import build
from retrying import retry

from helper.project_helper import get_folder_project_root_path
from helper.random_helper import get_random_in_array


class GSheetHelper:
    def __init__(self, *args, **kwargs):
        self.MAX_COLUMN = 'ZZ'
        self.MAX_ROW = '5000000'
        self.SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
        # self.gsheet_key_path = kwargs.get('gsheet_token_file',f'{get_folder_project_root_path()}/constant/gcloud/key/bee-gsheet.json')

        gsheet_file_path = get_random_in_array([
            # f'{get_folder_project_root_path()}/constant/gcloud/key/datanee-storage-51420a8f7fb3.json',
            f'{get_folder_project_root_path()}/constant/gcloud/key/bee-gsheet.json'
        ])
        self.gsheet_key_path = kwargs.get('gsheet_token_file', gsheet_file_path)
        # self.gsheet_key_path = kwargs.get('gsheet_token_file',
        #                                   f'{get_folder_project_root_path()}/constant/gcloud/key/bee-gsheet-new.json')

    @property
    def get_gsheet_service(self):
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = self.gsheet_key_path
        # print(os.environ['GOOGLE_APPLICATION_CREDENTIALS'])
        # print(f'gsheet_key_path: {self.gsheet_key_path}')
        return build(serviceName='sheets', version='v4', cache_discovery=False).spreadsheets()

    def get_sheet_id_by_url(self, spreadsheet_url):
        x = re.search(r"/spreadsheets/d/([a-zA-Z0-9-_]+)", spreadsheet_url)
        return x.group(1)

    def get_worksheet_id_from_name(self, spreadsheet_id, worksheet_title):
        service = self.get_gsheet_service
        sheet_metadata = service.get(spreadsheetId=spreadsheet_id).execute()
        properties = sheet_metadata.get('sheets')
        for item in properties:
            if item.get("properties").get('title') == worksheet_title:
                sheet_id = (item.get("properties").get('sheetId'))
                return sheet_id
        return None

    def create_worksheet(self, spreadsheet_id, title):
        body = {
            'requests': [{
                'addSheet': {
                    'properties': {
                        'title': title,
                        'sheetType': 'GRID',
                    }
                }
            }]
        }
        service = self.get_gsheet_service
        return service.batchUpdate(body=body, spreadsheetId=spreadsheet_id).execute()

    def init_worksheet_with_title(self, spreadsheet_id, worksheet_title, titles):
        self.create_worksheet(spreadsheet_id=spreadsheet_id, title=worksheet_title)
        self.append(spreadsheet_id=spreadsheet_id, worksheet_name=worksheet_title, data=titles)

    # data is [ [1,2,3,4], [5,6,7,8] ]
    def append(self, spreadsheet_id, worksheet_name, data):
        body = {
            'values': data
        }
        service = self.get_gsheet_service

        _range = f'{worksheet_name}!A1:{self.MAX_COLUMN}1'
        result = service.values().append(spreadsheetId=spreadsheet_id, range=_range,
                                         insertDataOption='INSERT_ROWS',
                                         valueInputOption='USER_ENTERED', body=body).execute()
        print('{0} cells appended.'.format(result \
                                           .get('updates') \
                                           .get('updatedCells')))
        return True

    def get_values_by_row(self, spreadsheet_id, worksheet_name, row):
        service = self.get_gsheet_service
        range_ = f'{worksheet_name}!A{row}:{self.MAX_COLUMN}{row}'
        result = service.values().get(spreadsheetId=spreadsheet_id, range=range_).execute()
        return result.get('values', [])

    @retry(wait_fixed=2000, stop_max_attempt_number=3)
    def append_with_title(self, spreadsheet_id, worksheet_name, data, title_row=1):
        worksheet_id = self.get_worksheet_id_from_name(spreadsheet_id=spreadsheet_id, worksheet_title=worksheet_name)
        if worksheet_id is None:
            # init worksheet
            self.init_worksheet_with_title(spreadsheet_id=spreadsheet_id, worksheet_title=worksheet_name, titles=
            [list(data.keys())])

        titles = self.get_values_by_row(spreadsheet_id=spreadsheet_id, worksheet_name=worksheet_name,
                                        row=title_row)
        if titles:
            titles = titles[0]
            data_refine = []
            for index, title in enumerate(titles):
                if title in data:
                    value = data.get(title)
                    if value is None:
                        value = ''
                    data_refine.append(value)
                else:
                    data_refine.append('')
            # print(data_refine)
            self.append(spreadsheet_id=spreadsheet_id, worksheet_name=worksheet_name, data=[data_refine])
            return True

    def append_with_title_auto_separated(self, spreadsheet_id, worksheet_name, data, title_row=1, max_row=20000):
        service = self.get_gsheet_service
        sheets = service.get(spreadsheetId=spreadsheet_id).execute().get('sheets')
        # find worksheet name all part
        lst_worksheet_exist = [sheet for sheet in sheets if
                               sheet.get('properties').get('title').startswith(worksheet_name)]
        lst_worksheet_exist.sort(key=lambda sheet: sheet.get('properties').get('title'))
        if len(lst_worksheet_exist) == 0:
            print('create new sheet')
            return self.append_with_title(spreadsheet_id, f'{worksheet_name}__1', data, title_row=title_row)
        else:
            sheet = lst_worksheet_exist[-1]
            if sheet.get('properties').get('gridProperties').get('rowCount') - 1000 <= max_row:
                print('append sheet', sheet.get('properties').get('title'))
                return self.append_with_title(spreadsheet_id, sheet.get('properties').get('title'), data,
                                              title_row=title_row)
            else:
                worksheet_name_new = ''
                if '__' in sheet.get('properties').get('title'):
                    worksheet_name_new = sheet.get('properties').get('title').split('__')[0] + '__' + str(
                        int(sheet.get('properties').get('title').split('__')[-1]) + 1)
                else:
                    worksheet_name_new = sheet.get('properties').get('title') + '__2'
                print('create sheet part ', worksheet_name_new)
                return self.append_with_title(spreadsheet_id, worksheet_name_new, data, title_row=title_row)

    # values is [4,3,1,2]
    def update_row(self, spreadsheet_id, worksheet_name, row_name, values, start_column='A'):

        body = {
            'values': [values]
        }
        service = self.get_gsheet_service
        _range = f'{worksheet_name}!{start_column}{row_name}:{self.MAX_COLUMN}{row_name}'
        print(_range)
        result = service.values().update(spreadsheetId=spreadsheet_id, range=_range,
                                         valueInputOption='USER_ENTERED', body=body).execute()
        print(f'{result.get("updatedRows")} updated rows with {result.get("updatedColumns")} updated columns')
        return True

    # values is [4,3,1,2]
    def update_column(self, spreadsheet_id, worksheet_name, column_name, values, start_row=1):

        data = []
        for value in values:
            data.append([value])
        body = {
            'values': data
        }
        service = self.get_gsheet_service
        _range = f'{worksheet_name}!{column_name}{start_row}:{column_name}{self.MAX_ROW}'
        print(_range)
        result = service.values().update(spreadsheetId=spreadsheet_id, range=_range,
                                         valueInputOption='USER_ENTERED', body=body).execute()
        print(f'{result.get("updatedRows")} updated rows with {result.get("updatedColumns")} updated columns')
        return True

    # value is number or string
    def update_value(self, spreadsheet_id, worksheet_name, column_name, row_name, value):

        body = {
            'values': [[value]]
        }
        service = self.get_gsheet_service
        _range = f'{worksheet_name}!{column_name}{row_name}:{column_name}{row_name}'
        print(_range)
        result = service.values().update(spreadsheetId=spreadsheet_id, range=_range,
                                         valueInputOption='USER_ENTERED', body=body).execute()
        print(f'{result.get("updatedRows")} updated rows with {result.get("updatedColumns")} updated columns')
        return True

    def create_sheet(self, title):
        spreadsheet = {
            'properties': {
                'title': title
            }
        }
        sheet_service = self.get_gsheet_service
        spreadsheet = sheet_service.create(body=spreadsheet,
                                           fields='spreadsheetId').execute()
        return spreadsheet


if __name__ == '__main__':
    gsheet_client = GSheetHelper()
    data = {'Date': '30/11/2021', 'Total product crawled': 169621, 'Total product uniq': 158852}
    gsheet_client.append_with_title(spreadsheet_id="1Uy7tcmGYgCvxb93pGHGrI20MGurlCEe1QmPAd0P9k3s",
                                    worksheet_name="1688",
                                    title_row=1,
                                    data=data)
