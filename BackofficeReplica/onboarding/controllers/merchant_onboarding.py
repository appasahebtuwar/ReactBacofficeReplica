from onboarding.models import HDFCUPIOnboard
from onboarding.serializers.merchant_onboarding_serializers import UPIMerchantOnboardSerializer, UPIMerchantListSerializer
from django.http import HttpResponse
# from shared.utility.custom_pagination import CustomPagination
# from shared.decorators import check_permissions
# from shared.utility.mixins import CustomResponseMixin
from rest_framework.response import Response
from django.conf import settings
# from shared.services.file.service import FileService
# from shared.services.file.params.excel import ExcelParams
import requests, json, os
from dateutil import parser
from datetime import datetime
# from pg.services.merchants import vaidate_ebz_merchants
# from shared.utility.loggers import Logger
# from pg.services.upi_merchant_onboarding.hdfc.validations import ValidationService
# from pg.services.encrypt_decrypt.hdfc_upi_onboarding import AESTool
from pathlib import Path
import pandas as pd


ENDPOINTS = {
    "dev":{
        "hdfc_upi_onboarding": "https://upitestv2.hdfcbank.com/UPI_MER_ONB/mer/realTimeMerOnboarding"
    },
    "prod":{
         "hdfc_upi_onboarding": "https://upiv2.hdfcbank.com/UPI_MER_ONB/mer/realTimeMerOnboarding"
    }
}

class HDFCUPIMerchantOnboardController:
    '''
    HDFCUPIMerchantOnboardController class perform the below things
        1. HDFC UPI Merchant onboarding through the API
        2. Deactivate the Merchant VPA
        3. Reactivate the merchant VPA
        4. List merchant all Merchant view
    '''
    serializer_class = UPIMerchantOnboardSerializer
    pagination_class = CustomPagination


    def __init__(self, *args, **kwargs):
        self.ENDPOINTS = ENDPOINTS[settings.ENV]   
        self.hdfc_upi_merchant_mid = settings.HDFCUPI_PNTPG_MID
        self.merchant_payout_ifsc = settings.HDFCUPI_MERPAYOUT_IFSC
        self.merchant_current_acc = settings.HDFCUPI_MER_CURRENT_AC
        self.msf_account_no = settings.HDFCUPI_MSF_ACNO
        self.msf_acc_ifsc = settings.HDFCUPI_MSFAC_IFSC
        self.parent_pg_merchant_id = settings.HDFCUPI_PNTPG_MID
        self.mandate_request_url = settings.HDFCUPI_MANDATE_REQUEST_URL
        self.upi_request_url = settings.HDFCUPI_REQUEST_URL
        self.hdfc_upi_enc_key = settings.HDFCUPI_ENC_KEY
        self.hdfc_upi_iv = settings.HDFCUPI_IV
        self.hdfc_upi_qnique_key_id = settings.HDFCUPI_UNIQUE_KEY_ID
        self.hdfc_upi_exttid = settings.HDFCUPI_EXTTID
        self.hdfc_upi_extmid = settings.HDFCUPI_EXTMID
        self.ca_chain_cert = settings.PG_STATIC_FILES_DIRECTORY + 'hdfc_upi_onboarding_ssl_cert/chain.crt'
        self.keyfile = settings.PG_STATIC_FILES_DIRECTORY + 'hdfc_upi_onboarding_ssl_cert/easebuzz.-prod.key'
        self.channel_id = settings.HDFCUPI_CHANNEL_ID

        # self.hdfc_upi_onboarding = "https://upiv2.hdfcbank.com/UPI_MER_ONB/mer/realTimeMerOnboarding" # PROD URL      
        # self.hdfc_upi_onboarding = "https://upitestv2.hdfcbank.com/UPI_MER_ONB/mer/realTimeMerOnboarding" #UAT URL


    def get_filtered_queryset(self, data, order_by="-id"):

        filter_args = {}

        if data.get("create_date_start", None) and data.get("create_date_end", None):
            filter_args["create_date__gte"] = data["create_date_start"]
            filter_args["create_date__lte"] = data["create_date_end"]

        data_field = ["id", "ebz_merchant_id", "ebz_submerchant_id", "hdfc_upi_merchant_id", "merchant_buss_name", "hdfc_upi_merchant_vpa", "hdfc_upi_merchant_mcc", "pan_num", "merchant_gst_no", "online_offline", "upi_create_date","is_active" ]

        for word in data_field:
            field_value = data.get(word, None)
            if field_value is not None:
                filter_args[word] = field_value
    
        queryset = HDFCUPIOnboard.objects.using('pg').filter(**filter_args) 
        return queryset.order_by(order_by)
    
    @check_permissions(["MANAGE_HDFC_UPI_ONBOARDING"])
    def list(self, request, *args, **kwargs):
        '''Returns a list of all the onboarded merchants '''
        pagination = CustomPagination(request.GET)
        queryset = self.get_filtered_queryset(request.GET)
        return pagination.get_paginated_response(queryset, UPIMerchantListSerializer, fields=request.GET.getlist("fields", None))
    
    @check_permissions(["MANAGE_HDFC_UPI_ONBOARDING"])
    def retrieve(self, request, *args, **kwargs):
        '''Returns the details of a specific onboarded merchant '''
        filter_args = {"id": kwargs.get("id")}
        queryset = self.get_filtered_queryset(filter_args).last()
        response_queryset = UPIMerchantOnboardSerializer(queryset).data
        response_queryset.update({"parent_pg_merchant_id": '*' * (len(response_queryset.get("hdfc_upi_merchant_id")) - 6) + response_queryset.get("hdfc_upi_merchant_id")[-6:]})
        if not queryset:
            Logger.info(extra={"UDF1": 'PG'}, message=f'HDFC Merchant not found. MID :: {str(filter_args["id"])}')
            return Response({"message": "Invalid ID."}, status=400)
        return Response({'hdfc_merchant_data':response_queryset})
    
    @check_permissions(["MANAGE_HDFC_UPI_ONBOARDING"])
    def create_hdfc_upi_merchant(self, request, *args, **kwargs):
        '''
        Create_hdfc_upi_merchant :
        1) receives merchant data from frontend
        2) validates data
        3) executes HDFC upi onboarding API and saves data in DB 
        '''
        response = {}
        Logger.info(message='HDFC Merchant onboarding api initiated', extra={"UDF1": 'PG'})
        try:
            # Merchant details validation
            merchant_data = request.data.get('merchant_details')[0]

            ebz_merchant_id = merchant_data["ebz_merchant_id"].strip()            
            ebz_submerchant_id = merchant_data["ebz_submerchant_id"].strip()

            #Validate ebz merchant id and ebz submerchant id
            resp = vaidate_ebz_merchants(ebz_merchant_id, ebz_submerchant_id)
            if not resp["success"]:
                return Response({"message":resp.get('message')}, status=400)  
            
            #changing pin code to str 
            merchant_data["st_pin_code"] = str(merchant_data["st_pin_code"])

            #changing request url to lowercase
            merchant_data["collect_request"] = merchant_data["collect_request"].lower() if "collect_request" in merchant_data else None


            # Validate merchant data
            validation_service = ValidationService()
            result = validation_service.validate(**{'record': merchant_data})

            if result['valid']:
                response['status'] = False
                response['errors'] = result['error']
                Logger.info(extra={"UDF1": 'PG',"errorCode": 400}, message=f'HDFC Merchant onboarding invalid data- {response.get("errors")}')
                return Response({"message":response.get('errors')}, status=400)
            
            #add suffix- ".easebuzz@hdfcbank" to the VPA 
            merchant_data["hdfc_upi_merchant_vpa"] = (merchant_data["hdfc_upi_merchant_vpa"].strip().lower() + ".esbz@hdfcbank")
          
            # # For Mandate
            request_url = self.mandate_request_url
            
            #  # For UPI
            if merchant_data['parent_pg_merchant_id'] == "upi":
                request_url = self.upi_request_url

            request_payload_data = {
                "account_type_flag": "Current",
                "modify_flag": "A",
                "merchant_payout_period": "T+1",
                "upi_create_date": str(datetime.now()),
                "settle_type": "NET",
                "fee_post_period": "Daily",
                "int_app": "WEBAPI",
                "legal_name": "EASEBUZZ PRIVATE LIMITED",
                "channel_id": self.channel_id,
                "merchant_payout_bankname": "HDFCBankPvtLtd",
                "merchant_payout_ifsc": self.merchant_payout_ifsc,
                "funcId": "0",
                "recSeq": "0",
                "merchant_current_acc": self.merchant_current_acc,
                "mpr_email_id": "bankingops@easebuzz.in",
                "annual_turn_over": 10000000,
                "merchant_type_flag": "SM",
                "one_time_fee_amount": 0,
                "maintenance_fee_amount": 0,
                "maintenance_fee_frequency": "N",
                "ext_tid": self.hdfc_upi_exttid,
                "ext_mid": self.hdfc_upi_extmid,
                "msf_account_no": self.msf_account_no,
                "msf_acc_ifsc": self.msf_acc_ifsc,
                "payout_hold_flag": "N",
                "fee_type": "M",
                "parent_pg_merchant_id": self.parent_pg_merchant_id,
                "retry": "N",
                "query": request_url,
                "collect_response": request_url,
                "refund_request": request_url
            }

            merchant_data.update(request_payload_data)
            merchant_onboard = self.call_hdfc_upi_onboard_api(merchant_data)
            Logger.info(extra={'UDF1': "PG"}, message=f"HDFC merchant onboard data:: {merchant_onboard}")

            if merchant_onboard['status']:   
                if merchant_onboard.get('data').get('status') == "SUCCESS":                    
                    merchant_data['hdfc_upi_merchant_key'] = self.hdfc_upi_enc_key
                    merchant_data['hdfc_upi_merchant_id'] = merchant_onboard.get('data').get('pgmerchant_Id')
                    Logger.info(extra={"UDF1": 'PG'}, message=f'HDFC Merchant onboarding API executed. HDFC UPI Merchant ID :: {merchant_data["hdfc_upi_merchant_id"] }')
                    merchant_data['is_active'] = True
                    merchant_data['updated_by'] = request.user_id

                    self.createHDFCUPIObj(**{'merchant_details':merchant_data})

                    message = merchant_onboard.get('data').get('message','Merchant onboarded successfully')
                    return Response({"message":message}, status=200)
                else:
                    error_code = merchant_onboard.get('data').get('error_code')
                    error_desc = validation_service.getErrorDetails(error_code)
                    possible_errors_list = error_desc['possible_errors']

                    errors = " or ".join(map(str,possible_errors_list))
                    response_msg = merchant_onboard.get('data').get('message')
                    message = f" {response_msg} or " + f"Possible errors are ({errors})"
                    return Response({"message":message}, status=400)
            
            message = merchant_onboard['data']
            return Response({"message":message}, status=400)

        except Exception as e:
            Logger.exception(extra={'UDF1': "PG"}, message=f"HDFC Create API EXCEPTION :: {str(e)}")
            return Response({"message":str(e)}, status=400)

    def call_hdfc_upi_onboard_api(self, bank_info):
        '''
        Hits the bank API for merchant onboarding
        '''
        Logger.info(extra={"UDF1": 'PG'}, message=f'request payload >>> {bank_info}')
        try:
            json_data = self.getHDFCUPIRequestPayload(bank_info)

            enc_dec_service = AESTool(MerchantKey=self.hdfc_upi_enc_key)
            encrypted_data = enc_dec_service.encrypt_upi_request(json.dumps(json_data))
            Logger.info(extra={"UDF1": 'PG'}, message=f'HDFC Encrypted payload generated, encrypted data >>> {encrypted_data}')


            hdfc_request_params = {
                "seq_number":str(json_data['seq_number']),
                "data": encrypted_data, 
                "key_id":0,
                "V":self.hdfc_upi_iv,
                "channel_id":self.channel_id,
            }
            
            api_response = self.requestURL(**{'hdfc_request_params':hdfc_request_params})
            Logger.info(extra={"UDF1": 'PG'}, message=f'Received API response >>> {api_response}')

            if api_response.status_code == 200:
                try:
                    data = json.loads(api_response.content)['data']
                    decrypt_data = enc_dec_service.decrypt_upi_response(data)
                    result = json.loads(decrypt_data)
                    Logger.info(extra={"UDF1": 'PG'}, message=f'Decrypted api response ({api_response.status_code}) >>> {result}')
                    return {'status':True,'data': result}
                except Exception as e:
                    Logger.exception(extra={'UDF1': "PG"}, message=f"HDFC Bank Onboarding API response ({api_response.status_code}) decryption EXCEPTION :: {str(e)}")
                    data = api_response.content.decode()
                    return {'status':False, 'data': data}
            else:
                return {'status':False, 'data': api_response.text}
        except Exception as e:
            Logger.exception(extra={'UDF1': "PG"}, message=f"HDFC Bank Onboarding API EXCEPTION :: {str(e)}")
            return {'status':False, 'data': str(e)}
        
    def getHDFCUPIRequestPayload(self, bank_info, action=None):

        if action=="deactivate_merchant":
            upi_create_date = parser.parse(str(bank_info['upi_create_date'])).strftime('%Y-%m-%d %H:%M:%S')
            modify_flag = "D"
            one_time_fee_amount = int(bank_info['one_time_fee_amount'])
            maintenance_fee_amount = int(bank_info['maintenance_fee_amount'])
            annual_turn_over = int(bank_info['annual_turn_over'])
        else:
            upi_create_date = parser.parse(bank_info['upi_create_date']).strftime('%Y-%m-%d %H:%M:%S')
            modify_flag = str(bank_info['modify_flag'])
            one_time_fee_amount = str(bank_info['one_time_fee_amount'])
            maintenance_fee_amount = str(bank_info['maintenance_fee_amount'])
            annual_turn_over = str(bank_info['annual_turn_over'])
            
        json_data = {
            "MEBUSSNAME":str(bank_info['merchant_buss_name']),
            "LEGALSTRNAME": str(bank_info['legal_name']),
            "STADD1":str(bank_info['stadd1']),
            "STCITY":str(bank_info['Stcity']),
            "MCC":str(bank_info['hdfc_upi_merchant_mcc']),
            "MERACCNO":str(bank_info['merchant_current_acc']),
            "ownerName":str(bank_info['owner_name']),
            "ownership":str(bank_info['ownership']),
            "CNTEMAIL":str(bank_info['mpr_email_id']),
            "turnover":annual_turn_over,
            "mertypeflag":str(bank_info['merchant_type_flag']),
            "Pntpgmerchantid":str(bank_info['parent_pg_merchant_id']),
            "merVirtualAdd":str(bank_info['hdfc_upi_merchant_vpa']),
            "FEETYPE":str(bank_info['fee_type']),
            "INTAPP":str(bank_info['int_app']),
            "STSTATE":str(bank_info['st_state']),
            "STPINCODE":str(bank_info['st_pin_code']),
            "CNTNAME":str(bank_info['merchant_contact_name']),
            "CNTPHONE":str(bank_info['merchant_contact_phone']),
            "CNTMOBILE":str(bank_info['user_mobile_no']),
            "MERISSUEBNK":str(bank_info['merchant_payout_bankname']), 
            "MERIFSCCODE": str(bank_info['merchant_payout_ifsc']),
            "OneTimeFreeAmount":one_time_fee_amount,
            "MaintenanceFeeFrequency":str(bank_info['maintenance_fee_frequency']), 
            "MaintenanceFeeAmount":maintenance_fee_amount,
            "gstn":str(bank_info['merchant_gst_no']),
            "PAYPERIODTYPE":str(bank_info['merchant_payout_period']),
            "SETTLETYPE":str(bank_info['settle_type']),
            "FEEPOSTPERIOD":str(bank_info['fee_post_period']),
            "EXTTID":str(bank_info['ext_tid']),
            "EXTMID":str(bank_info['ext_mid']),
            "MAXTRANAMNT":str(bank_info['max_per_day_txn_limit']),
            "PERTRANLIMIT":str(bank_info['per_txn_limit']),
            "MSFACCNO":str(bank_info['msf_account_no']),
            "MSFIFSCCODE":str(bank_info['msf_acc_ifsc']),
            "MAXTRANS":str(bank_info['max_txn_limit']),
            "funcId":str(bank_info['funcId']),
            "recSeq":str(bank_info['recSeq']),
            "chnid":str(bank_info['channel_id']),
            "reqFlag":modify_flag,
            "metrntype":str(bank_info['online_offline']),
            "acctypeflag":str(bank_info['account_type_flag']),
            "payouthld":str(bank_info['payout_hold_flag']),
            "panNumber":str(bank_info['pan_num']),
            "PANRegDate": datetime.strptime(str(bank_info['pan_reg_date']), '%Y-%m-%d').strftime('%d-%m-%Y'),
            "seq_number":str(bank_info['seq_number']),
            "crtDate":upi_create_date,
            "REQUESTURL1":str(bank_info['collect_request']),
            "REQUESTURL2":str(bank_info['collect_response']),
            "REQUESTURL3":str(bank_info['query']),
            "REQUESTURL4":str(bank_info['refund_request']),
            "retry":str(bank_info['retry']),
        }
        return json_data
        
    def requestURL(self, **kwargs):        
        onboard_url = self.ENDPOINTS['hdfc_upi_onboarding'] # hdfc api UAT endpoint with ssl certs
        data = kwargs['hdfc_request_params']
        
        response = requests.request(
            method="POST",
            url=onboard_url,
            json=data,
            cert=(self.ca_chain_cert, self.keyfile),
            verify=True,
            timeout=30,
        )
        Logger.info(extra={"UDF1": 'PG'}, message='HDFC Onboarding API executed')
        return response

    def createHDFCUPIObj(self, *args, **kwargs):
        try:
            create_obj_args = kwargs.get('merchant_details')
            hdfc_upi_obj = HDFCUPIOnboard.objects.using('pg').create(**(create_obj_args))
            Logger.info(extra={"UDF1": 'PG'}, message='HDFC Onboarding data saved in DB')
            return {'status':True, 'created_obj_id':hdfc_upi_obj.id}
        except Exception as e:
            print(e)
            return {'status':False, 'message':str(e)}


    @check_permissions(["MANAGE_HDFC_UPI_ONBOARDING"])
    def deactivateHDFCUPIMerchant(self, request):
        try:
            merchantid = request.data.get('merchant_id')
            merchant_obj = HDFCUPIOnboard.objects.using('pg').filter(id=merchantid)
            merchant_details = merchant_obj.values().first()
            
            request_payload = self.getHDFCUPIRequestPayload(bank_info=merchant_details, action="deactivate_merchant")
            Logger.info(extra={"UDF1": 'PG'}, message=f'HDFC Deactivate Merchant Request Payload >>> {request_payload}')
            
            enc_dec_service = AESTool(MerchantKey=self.hdfc_upi_enc_key)
            encrypted_data = enc_dec_service.encrypt_upi_request(json.dumps(request_payload))

            hdfc_request_params = {
                "seq_number":str(request_payload['seq_number']),
                "data": encrypted_data, 
                "key_id":0,
                "V":self.hdfc_upi_iv,
                "channel_id":self.channel_id,
            }
            

            
            api_response = self.requestURL(**{'hdfc_request_params':hdfc_request_params})
            Logger.info(extra={"UDF1": 'PG'}, message=f' HDFC Received Deactivate API response >>> {api_response}')

            if api_response.status_code == 200:
                try:
                    data = json.loads(api_response.content)['data']
                    decrypt_data = enc_dec_service.decrypt_upi_response(data)
                    result = json.loads(decrypt_data)
                    Logger.info(extra={"UDF1": 'PG'}, message=f'HDFC Decrypted Deactivate Api response ({api_response.status_code}) >>> {result}')

                    if result.get('status') == "SUCCESS":
                        merchant_obj = merchant_obj.first()
                        merchant_obj.is_active = False
                        merchant_obj.updated_by = "Onboarding Team" #request.user_id
                        merchant_obj.modify_flag = "D"
                        merchant_obj.save()
                        message = 'Merchant deactivated successfully'
                        return Response({"message":message}, status=200)
                    
                    return Response({"message":"Something went wrong"}, status=400)
                except Exception as e:
                    Logger.exception(extra={'UDF1': "PG"}, message=f"HDFC Deactivate Merchant API response ({api_response.status_code}) decryption EXCEPTION :: {str(e)}")
                    data = api_response.content.decode()
                    return Response({"message":str(e)}, status=400)
            else:
                return Response({"message":api_response.text}, status=400)

        except Exception as e:
            Logger.exception(extra={'UDF1': "PG"}, message=f"HDFC Deactivate Merchant API Exception :: {str(e)}")
            return Response({"message":str(e)}, status=400)
        
    def download_report(self, request, *args, **kwargs):
        '''
        Downloads report of the Merchant Onboarding data 
        '''
        try:
            data = request.data
            columns = data.get("columns")
            columns = [col for col in columns if col != 'id']
            merchants_onboarded = self.get_filtered_queryset( data)
            serialized_data = HDFCDownloadReportSerializer(merchants_onboarded, include_fields=columns, many=True).data
            
            column_mapping = { 
                "id"                    : "S. No",
                "ebz_merchant_id"       : "Ebz Merchant ID",
                "ebz_submerchant_id"    : "Ebz Submerchant ID",
                "hdfc_upi_merchant_id"  : "HDFC MID",
                "ext_tid"               : "Terminal ID",
                "ext_mid"               : "External Merchant ID",
                "merchant_buss_name"    : "Business Name",
                "hdfc_upi_merchant_mcc" : "MCC",
                "hdfc_upi_merchant_vpa" : "VPA",
                "mpr_email_id"          : "Email ID",
                "merchant_contact_name" : "Contact Name",
                "collect_request"       : "Merchant URL",
                "pan_num"               : "PAN number",
                "merchant_gst_no"       : "GST no",
                "merchant_type_flag"    : "Merchant Type Flag",
                "ownership"             : "Business Type",
                "online_offline"        : "Merchant TAG",
                "user_mobile_no"        : "Mobile No",
                "Stcity"                : "City",
                "st_state"              : "State",
                "create_date"           : "Create Date",
                "updated_by"            : "Created By",
                "is_active"             : "Active"
            }

            field=[]
            for f in columns:
                if f in column_mapping.keys():
                    field.append(column_mapping[f])
            report_service = FileService()
            data = list(serialized_data)
            df = pd.DataFrame(data)
            df['is_active'] = df['is_active'].apply(lambda x: 'active' if x else 'inactive')
            data = df.to_dict(orient='records')
            
            #ExcelParams function - validates the data and the column field and sets directory location for the file to be saved"
            
            try:
                excel_params = ExcelParams(
                    data=data,
                    columns=field,
                )
            except Exception as e:
                Logger.error( extra={'UDF1': "PG"}, message=f"HDFC Merchant Onboarding DOWNLOAD REPORT ERROR :: {str(e)}")
                return Response({"success": False, "message": str(e)})
            
            
            #generate_excel_report function - generates an Excel report using Pandas with provided data and columns information
            
            service_response = report_service.generate_excel_report(excel_params)
            if not service_response.get("success"):
                return Response({"success": False, "message": service_response.get("message")})          
            file_path   = service_response.get("file_path")

            response    = HttpResponse( content_type="application/xlsx")
            response['Content-Disposition'] = 'attachment'

        
            #The below function reads the xlsx file to serve it as a binary file in HTTP response
            
            file = open(file_path, "rb")
            response.write(file.read())
            file.close()
            if(file_path):
                file_to_remove = Path(file_path)
                file_to_remove.unlink()

            return response

        except Exception as e:
            Logger.exception( extra={'UDF1': "PG"}, message=f"HDFC Merchant Onboarding DOWNLOAD REPORT EXCEPTION :: {str(e)}")
            return Response({"success": False, "message": "Something went wrong."})
    