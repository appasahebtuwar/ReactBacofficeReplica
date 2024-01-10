from django.urls import path
from onboarding.controllers.merchant_onboarding import HDFCUPIMerchantOnboardController
from onboarding.controllers.collective.merchant_onboarding_utils import EbzMerchantsOnboardController
from rest_framework.urlpatterns import format_suffix_patterns

internal_routes = [
    path('perations/hdfc/merchants/', HDFCUPIMerchantOnboardController.as_view({'get':'list'}),name="hdfcupi-merchants-list"),
    path('perations/hdfc/merchants/<str:id>/', HDFCUPIMerchantOnboardController.as_view({'get' : 'retrieve'}), name="hdfcupi-merchant-retrieve"),
    path('perations/hdfc/createMerchant/', HDFCUPIMerchantOnboardController.as_view({'post': 'create_hdfc_upi_merchant'}), name="create-hdfc-upi-merchant"),
    path('perations/hdfc/deactivateMerchant/', HDFCUPIMerchantOnboardController.as_view({'post': 'deactivateHDFCUPIMerchant'}), name="deactivate-hdfc-upi-merchant"),
    path('perations/hdfc/download_report/', HDFCUPIMerchantOnboardController.as_view({'post':'download_report'}),name="download-report"),
    
    #For HDFC Onboarding
    path('hdfc/merchant/<str:id>/', EbzMerchantsOnboardController.as_view({'get' : 'get_merchant'}), name="ebz-merchant-retrieve"),
    path('hdfc/submerchant/', EbzMerchantsOnboardController.as_view({'post' : 'get_submerchant'}), name="ebz-submerchant-retrieve"),
]


urlpatterns = internal_routes
urlpatterns = format_suffix_patterns(urlpatterns)
