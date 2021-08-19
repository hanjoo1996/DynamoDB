import pygsheets
import boto3
import json
from pprint import pprint
from boto3.dynamodb.conditions import Key
from boto3.dynamodb.types import TypeDeserializer

# dynamoDB marshal to JSON
def ddb_deserialize(r, type_deserializer = TypeDeserializer()):
    return type_deserializer.deserialize({"M": r})

def scan_string(year=2, keyword=None, format="", business_reg =""):

    client = boto3.client('dynamodb')

    input_string = 'input params :'

    if (year):
        input_string += ' year : {} '.format(year)
    if (keyword):
        input_string += ' keyword : {} '.format(keyword)
    if (format):
        input_string += ' format : {} '.format(format)
    if (business_reg):
        input_string += ' business_reg : {} '.format(business_reg)
        
    print(input_string)

    filter_expression = ""

    attribute_names = {
        }

    attribute_vals = {
        }

    if business_reg:
        
        filter_expression += "contains (#business_reg, :business_reg)"
        attribute_names["#business_reg"] = '사업자등록번호'
        attribute_vals[":business_reg"] = {"S":str(business_reg)}
    
    if format:

        filter_expression += ' and contains (#format,:format)'
        attribute_names["#format"] = '신고서식'
        attribute_vals[":format"] = {"S":str(format)}

    if year:

        filter_expression += ' and contains (#yr,:yr)'
        attribute_names["#yr"] = '신고년도'
        attribute_vals[":yr"] = {"S":str(year)}

    filter_expression += " and attribute_not_exists(expired)"

    print("SCAN QUERY = {}".format(filter_expression))

    response = client.scan(
        TableName = 'ift_declare_hometax_data_stag',
        FilterExpression = filter_expression,
        ExpressionAttributeNames = attribute_names,
        ExpressionAttributeValues = attribute_vals
    )
    
    if (response['Items']):
        ret = response['Items']

        for i in range(len(ret)):
            if keyword:
                r_json = ddb_deserialize(ret[i])
                if keyword in r_json['declare_data']:
                    pprint(r_json)
                    return r_json
                    
            else:
                r_json = ddb_deserialize(ret[i])
                pprint(r_json)
                return r_json
    
    else:
        ret = response['Items']
        print("else {}".format(ret))
        return 0

    

    


if __name__ == '__main__':

    ########################################################################
    # python -> gSheets setup
    # need to locate OAuth file into current directory (client_secret.json)

    #client = pygsheets.authorize()

    # sh = client.open('v2.2 사유서 총괄틀의 사본')
    # s = sh.worksheet_by_title('사유서 링크')

    # pygsheet .cell function call
    # cell_print_function = s.cell('C48')

    # pygsheet .update_value function call
    # s.update_value('F47','test')

    ########################################################################
    # python -> DynamoDB setup

    
    test = scan_string(format="D102200",business_reg="6108124133")

    #pprint(test)

    
