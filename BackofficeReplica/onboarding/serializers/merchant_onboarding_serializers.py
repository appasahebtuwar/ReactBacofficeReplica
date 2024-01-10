from onboarding.models import UPIMerchantOnboardInfo
from rest_framework import serializers 


class UPIMerchantOnboardSerializer(serializers.ModelSerializer):

    class Meta:
        model = UPIMerchantOnboardInfo
        fields = "__all__"  



class UPIMerchantListSerializer(UPIMerchantOnboardSerializer):
    def __init__(self, instance=None, data=..., **kwargs):
        super().__init__(instance, data, **kwargs)

        class Meta:
            model = UPIMerchantOnboardInfo