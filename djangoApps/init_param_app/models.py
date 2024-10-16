from django.db import models
from django.utils import timezone


# Create your models here.
class CfeParams(models.Model):
    id = models.BigAutoField(primary_key=True)
    name = models.CharField()
    description = models.TextField(blank=True, null=True)
    units = models.CharField(blank=True, null=True)
    data_type = models.CharField(blank=True, null=True)
    limits = models.CharField(blank=True, null=True)
    role = models.CharField(blank=True, null=True)
    calibratable = models.BooleanField(blank=True, null=True)
    source_file = models.CharField(blank=True, null=True)
    min = models.CharField(blank=True, null=True)
    max = models.CharField(blank=True, null=True)
    nwm_name = models.CharField(blank=True, null=True)
    default_value = models.CharField(blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'cfe_params'
        db_table_comment = 'Table to contain soil parameters such as depth, satdk, etc found at confluence: https://confluence.nextgenwaterprediction.com/display/NGWPC/CFE for model CFE'



class HFFiles(models.Model):
    gage_id = models.CharField()
    hydrofabric_version = models.CharField()
    filename = models.CharField()
    uri = models.CharField()
    domain = models.CharField()
    data_type = models.CharField()
    source = models.CharField()
    module_id = models.CharField()
    ipe_json = models.CharField()
    update_time = models.DateTimeField(default=timezone.now)

    class Meta:
        managed = False
        db_table = 'restapi_hffiles'

    # method to return all fields
    def __str__(self):
        return self.gage_id
