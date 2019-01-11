import os
import json
import threading
import re
import requests
import traceback

from bs4 import BeautifulSoup
from confluent_kafka import Producer, KafkaError

def handler(context, event):

    b = event.body
    if not isinstance(b, dict):
        body = json.loads(b.decode('utf-8-sig'))
    else:
        body = b
    
    context.logger.info("Event received: " + str(body))

    try:

        # if we're not ready to handle this request yet, deny it
        if not FunctionState.done_loading:
            context.logger.warn_with('Function not ready, denying request !')
            raise NuclioResponseError(
                'The service is loading and is temporarily unavailable.',requests.codes.unavailable)
        
        # parse event's payload
        year, region = Helpers.parse_body(context,body)

        # special case for overseas region
        # cf. RPG Version 2.0 - Descriptif de contenu et de livraison (DC_DL_RPG_2-0.pdf)
        # http://professionnels.ign.fr/doc/DC_DL_RPG_2-0.pdf
        if region in [1,2,3,4,6]:
            region = "D97{0}".format(region)
        else:
            region = "R{0}".format(region)

        archive_regex = "RPG_2-0__SHP_.*_{1}-{0}_{0}-01-01.7z(.001)?$".format(year,region)

        # French LPIS (Registre Parcelaire Graphique / IGN)
        rpg_url = FunctionConfig.lpis_fr_ign_rpg_url        
        context.logger.info("French LPIS (IGN): {0}".format(rpg_url))

        r = requests.get(rpg_url)

        # parse response content
        bs = BeautifulSoup(r.content,'html.parser')
        hrefs = bs.findAll("a", {"class": "fleche2"})

        # search requested archive
        links = []
        for h in hrefs:
            if h.get('href') and re.search(archive_regex,h.get('href')):
                links.append(h.get('href'))

        # check if there is indeed archive to download
        if not len(links) == 1:
            context.logger.warn_with("Failed to scrape French LPIS archive for year '{0}' and region '{1}' (not found), regex={2}"
                .format(year,region,archive_regex))
            raise NuclioResponseError(
                "Failed to scrape French LPIS archive for year '{0}' and region '{1}' (not found)".format(year,region),requests.codes.bad)

        # check archive format
        archive_to_donwload = links[0]
        if not (archive_to_donwload.endswith('7z') or archive_to_donwload.endswith('7z.001')):
            context.logger.warn_with("Not a 7z archive, url(s)={0}".format(archive_to_donwload))
            raise NuclioResponseError(
                "Not a 7z archive, url(s)={0}".format(archive_to_donwload),requests.codes.bad)

        context.logger.info("French LPIS archive(s) for year '{0}' and region '{1}' successfully scraped".format(year,region))
        context.logger.info("Archive URL: {0}".format(archive_to_donwload))

        # set the source id
        source_id = "{0}:{1}".format(FunctionConfig.source_id,region)

        parcel_id_field = FunctionConfig.parcel_id_field
        normalized_parcel_properties = FunctionConfig.normalized_parcel_properties

        # forge download command
        download_cmd = {
            "format": "7z", 
            "url": archive_to_donwload,
            "sourceID": source_id,
            "parcelIdField": parcel_id_field,
            "normalizedProperties": normalized_parcel_properties,
            "version": year
        }

        # submit download command on a dedicated kafka topic
        p, target_topic = FunctionState.producer, FunctionConfig.target_topic
        value = json.dumps(download_cmd).encode("utf-8-sig")
        p.produce(target_topic, value)
        p.flush()

        context.logger.info('Download command successfully submitted: {0}'.format(value))

    except NuclioResponseError as error:
        return error.as_response(context)

    except Exception as error:
        context.logger.warn_with('Unexpected error occurred, responding with internal server error',
            exc=str(error))
        message = 'Unexpected error occurred: {0}\n{1}'.format(error, traceback.format_exc())
        return NuclioResponseError(message).as_response(context)

    return context.Response(body=json.dumps({"message": "Archive '{0}' successfully submitted for ingestion"
        .format(archive_to_donwload)}),
                            headers={},
                            content_type='application/json',
                            status_code=requests.codes.ok)


class FunctionConfig(object):

    kafka_bootstrap_server = None

    target_topic = None

    source_id = "lpis:fr"

    parcel_id_field = "ID_PARCEL"

    normalized_parcel_properties = dict([
        ('area',{'sourceProp': 'SURF_PARC', 'coefSI': '*10000'})])

    lpis_fr_ign_rpg_url = None


class FunctionState(object):

    producer = None

    done_loading = False


class Helpers(object):

    @staticmethod
    def load_configs():

        FunctionConfig.kafka_bootstrap_server = os.getenv('KAFKA_BOOTSTRAP_SERVER')

        FunctionConfig.target_topic = os.getenv('TARGET_TOPIC')

        FunctionConfig.lpis_fr_ign_rpg_url = os.getenv('LPIS_FR_IGN_RPG_URL','http://professionnels.ign.fr/rpg')

    @staticmethod
    def load_producer():

        p = Producer({
            'bootstrap.servers':  FunctionConfig.kafka_bootstrap_server})

        FunctionState.producer = p

    @staticmethod
    def parse_body(context, body):

        # parse year
        if not 'year' in body:
            context.logger.warn_with('Missing \'year\' attribute !')
            raise NuclioResponseError(
                'Missing \'year\' attribute !',requests.codes.bad)
        if not int(body['year']):
            context.logger.warn_with('\'year\' attribute must be an integer !')
            raise NuclioResponseError(
                '\'year\' attribute must be an integer !',requests.codes.bad)
        year = body['year']

        # parse province
        if not 'region' in body:
            context.logger.warn_with('Missing \'region\' attribute !')
            raise NuclioResponseError(
                'Missing \'region\' attribute !',requests.codes.bad)
        if not int(body['region']):
            context.logger.warn_with('\'region\' attribute must be an integer !')
            raise NuclioResponseError(
                '\'region\' attribute must be an integer !',requests.codes.bad)
        region = body['region']

        return year, region

    @staticmethod
    def on_import():

        Helpers.load_configs()
        Helpers.load_producer()
        FunctionState.done_loading = True


class NuclioResponseError(Exception):

    def __init__(self, description, status_code=requests.codes.internal_server_error):
        self._description = description
        self._status_code = status_code

    def as_response(self, context):
        return context.Response(body=json.dumps({"message": self._description}),
                                headers={},
                                content_type='application/json',
                                status_code=self._status_code)


t = threading.Thread(target=Helpers.on_import)
t.start()
