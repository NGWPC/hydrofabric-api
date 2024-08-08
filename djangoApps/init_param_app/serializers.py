from rest_framework import serializers

class ModelSerializer(serializers.Serializer):
    model_id = serializers.IntegerField()
    name = serializers.CharField(max_length=255)

# 
class InitialParameterSerializer(serializers.Serializer):
    name = serializers.CharField(max_length=255)
    units = serializers.CharField(max_length=255, allow_blank=True, allow_null=True)
    limits = serializers.CharField(max_length=255, allow_blank=True, allow_null=True)
    role = serializers.CharField(max_length=255, allow_blank=True, allow_null=True)
    description = serializers.CharField(max_length=255, allow_blank=True, allow_null=True)
    default = serializers.CharField(max_length=255, allow_blank=True, allow_null=True)
    calibratable = serializers.CharField(max_length=255, allow_blank=True, allow_null=True)
    datatype = serializers.CharField(max_length=255, allow_blank=True, allow_null=True)
    version = serializers.CharField(max_length=255, allow_blank=True, allow_null=True)
    mean = serializers.CharField(max_length=255, allow_blank=True, allow_null=True)
    std_dev = serializers.CharField(max_length=255, allow_blank=True, allow_null=True)
    minimum = serializers.CharField(max_length=255, allow_blank=True, allow_null=True)
    percent_25 = serializers.CharField(max_length=255, allow_blank=True, allow_null=True)
    percent_50 = serializers.CharField(max_length=255, allow_blank=True, allow_null=True)
    percent_75 = serializers.CharField(max_length=255, allow_blank=True, allow_null=True)
    maximum = serializers.CharField(max_length=255, allow_blank=True, allow_null=True)
    static = serializers.CharField(max_length=255, allow_blank=True, allow_null=True)
    type = serializers.CharField(max_length=255, allow_blank=True, allow_null=True)


class ModulesSerializer(serializers.Serializer):
    module_name = serializers.CharField(max_length=255)
    description = serializers.CharField(max_length=255, allow_blank=True, allow_null=True)
    groups = serializers.CharField(max_length=255, allow_blank=True, allow_null=True)
    version_url = serializers.CharField(max_length=255, allow_blank=True, allow_null=True)
    commit_hash = serializers.CharField(max_length=255, allow_blank=True, allow_null=True)
    version_number = serializers.CharField(max_length=255, allow_blank=True, allow_null=True)