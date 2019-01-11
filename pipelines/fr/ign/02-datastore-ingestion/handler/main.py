import os
import json
import threading
import requests
import traceback

from pymongo import MongoClient
from pyproj import Proj, transform

def handler(context, event):

    b = event.body
    if not isinstance(b, dict):
        body = json.loads(b.decode('utf-8-sig'))
    else:
        body = b
    
    context.logger.info("Event received !")

    try:

        # if we're not ready to handle this request yet, deny it
        if not FunctionState.done_loading:
            context.logger.warn_with('Function not ready, denying request !')
            raise NuclioResponseError(
                'The service is loading and is temporarily unavailable.',requests.codes.unavailable)

        # MongoDB client setup
        client = FunctionState.mongodb_client
        db = client[FunctionConfig.target_db] 
        c = db[FunctionConfig.target_collection]

        reproject = Helpers.reproject_coordinates

        id = body['_id']

        # Check if a CRS (Coordinate Reference System) transformation is needed
        if 'crs' in  body['properties']:
            if body['properties']['crs']['type'] == 'EPSG':
                code = body['properties']['crs']['properties']['code']
                if code != FunctionConfig.target_crs_epsg_code:
                    source_proj = Proj(init='EPSG:{0}'.format(code))
                    target_proj = Proj(init='EPSG:{0}'.format(FunctionConfig.target_crs_epsg_code))

                    new_coordinates = reproject(source_proj,target_proj,body['geometry']['coordinates'])                        
                    body['geometry']['coordinates'] = new_coordinates

        upsert_id = c.replace_one({ '_id': id }, body, upsert=True).upserted_id

        context.logger.info("LPIS feature '{0}' '{1}' successfully".format(
            id, 
            'inserted' if upsert_id is not None else 'updated'))

    except NuclioResponseError as error:
        return error.as_response(context)

    except Exception as error:
        context.logger.warn_with('Unexpected error occurred, responding with internal server error',
            exc=str(error))
        message = 'Unexpected error occurred: {0}\n{1}'.format(error, traceback.format_exc())
        return NuclioResponseError(message).as_response(context)

    return 

class FunctionConfig(object):

    mongodb_host = None

    mongodb_port = None

    target_db = None

    target_collection = None

    target_crs_epsg_code = None


class FunctionState(object):

    mongodb_client = None

    done_loading = False


class Helpers(object):

    @staticmethod
    def reproject_coordinates(source_proj,target_proj, coords):
        new_coords = []
        for c in coords:
            if isinstance(c,list):
                new_coords.append(Helpers.reproject_coordinates(source_proj,target_proj,c))
            else:
                return transform(source_proj,target_proj,*coords)
        return new_coords

    @staticmethod
    def load_configs():

        FunctionConfig.mongodb_host = os.getenv('MONGODB_HOST','localhost')

        FunctionConfig.mongodb_port = os.getenv('MONGODB_PORT',27017)

        FunctionConfig.target_db = os.getenv('TARGET_DB','fast')

        FunctionConfig.target_collection = os.getenv('TARGET_COLLECTION','lpis')

        FunctionConfig.target_crs_epsg_code = os.getenv('TARGET_CRS_EPSG_CODE','4326') # WGS84 -> EPSG:4326

    @staticmethod
    def load_mongodb_client():

        host = FunctionConfig.mongodb_host
        port = FunctionConfig.mongodb_port
        uri = "mongodb://{0}:{1}/".format(host,port)
        FunctionState.mongodb_client = MongoClient(uri)

    @staticmethod
    def on_import():

        Helpers.load_configs()
        Helpers.load_mongodb_client()
        FunctionState.done_loading = True


class NuclioResponseError(Exception):

    def __init__(self, description, status_code=requests.codes.internal_server_error):
        self._description = description
        self._status_code = status_code

    def as_response(self, context):
        return context.Response(body=self._description,
                                headers={},
                                content_type='text/plain',
                                status_code=self._status_code)


t = threading.Thread(target=Helpers.on_import)
t.start()
