from django.db import models
from django.utils import timezone


# Create your models here.
class HFFiles(models.Model):
    gage_id = models.CharField()
    hydrofabric_version = models.CharField()
    filename = models.CharField()
    uri = models.URLField()
    domain = models.CharField()
    data_type = models.CharField()
    source = models.CharField()
    module_id = models.CharField()
    update_time = models.DateTimeField(default=timezone.now)

    class Meta:
        #managed = False
        db_table = 'restapi_hffiles'

    # method to return all fields
    def __str__(self):
        return self.gage_id
