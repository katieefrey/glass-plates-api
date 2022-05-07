from typing import List, Optional, Any, Callable
from fastapi import FastAPI

from fastapi.types import DecoratedCallable

from astropy import units as u
from astropy.coordinates import SkyCoord

from bson.json_util import dumps
import pymongo
import json

import certifi

import secrets

my_client = pymongo.MongoClient(connect_string, tlsCAFile=certifi.where())
dbname = my_client['plates']
glassplates = dbname["glass"]
archives = dbname["archives"]

sort_list = [
    {
        "name" : "Identifier",
        "nickname" :"identifier",
        "field": "identifier"
    },
    {
        "name" : "Archive",
        "nickname" :"archive",
        "field": "archive"
    },
    {
        "name" : "Right Ascension",
        "nickname" : "ra",
        "field": "exposure_info.ra_deg"
    },
]

app = FastAPI()

# search plates
@app.get("/")
def search_plates(
    skip: int = 0,
    limit: int = 50,
    archive: Optional[str] = "all",
    identifier: Optional[str] = None,
    obj: Optional[str] = None,
    ra: Optional[str] = None,
    dec: Optional[str] = None,
    radius: Optional[str] = 10,
    text: Optional[str] = None,
    observer: Optional[str] = None,
    sort_order: Optional[str] = "identifier"
    ):
    
    radius = int(radius)/60

    query = {}
    
    if identifier != None:
        query["identifier"] = { "$regex" : identifier, "$options" : "i"}

    if archive != "all":
        query["archive"] = { "$regex" : archive, "$options" : "i"}

    if obj != None:
        try:
            coords = SkyCoord.from_name(obj)
            ra = coords.ra.deg
            dec = coords.dec.deg
        except:
            results = {
                "total" : 0,
                "limit" : limit,
                "skip" : skip,
                "results": []
            }
            return results

    if ra != None:
        try:
            if ":" in str(ra):
                coords = SkyCoord(ra+" 0", unit=(u.hourangle, u.deg))
                ra = coords.ra.deg
            minra = round(float(ra) - radius*15, 4)
            maxra = round(float(ra) + radius*15, 4)
            query["exposure_info"] = {"$elemMatch": {"ra_deg": {"$gt": minra, "$lt": maxra}}}
        except:
            results = {
                "total" : 0,
                "limit" : limit,
                "skip" : skip,
                "results": []
            }
            return results

    if dec != None:
        try:
            if ":" in str(dec):
                coords = SkyCoord("0 "+dec, unit=(u.hourangle, u.deg))
                dec = coords.dec.deg
            mindec = round(float(dec) - radius, 4)
            maxdec = round(float(dec) + radius, 4)
            query["exposure_info"] = {"$elemMatch": {"dec_deg": {"$gt": mindec, "$lt": maxdec}}}
        except:
            results = {
                "total" : 0,
                "limit" : limit,
                "skip" : skip,
                "results": []
            }
            return results

    if ra != None and dec != None:
        del query["exposure_info"]
        query["$and"] = [
            {"exposure_info": {"$elemMatch": {"dec_deg": {"$gt": mindec, "$lt": maxdec}}}},
            {"exposure_info": {"$elemMatch": {"ra_deg": {"$gt": minra, "$lt": maxra}}}}
        ]

    if text != None:
        
        query["$or"] = [
            {"plate_info.availability_note" : { "$regex" : text, "$options" : "i"}},
            {"plate_info.digitization_note" : { "$regex" : text, "$options" : "i"}},
            {"plate_info.quality" : { "$regex" : text, "$options" : "i"}},
            {"plate_info.notes" : { "$regex" : text, "$options" : "i"}},
            {"plate_info.condition" : { "$regex" : text, "$options" : "i"}},
            {"plate_info.observer" : { "$regex" : text, "$options" : "i"}},
            {"obs_info.instrument" : { "$regex" : text, "$options" : "i"}},
            {"obs_info.observatory" : { "$regex" : text, "$options" : "i"}},
            {"exposure_info.target" : { "$regex" : text, "$options" : "i"}},
            {"plate_info.emulsion" : { "$regex" : text, "$options" : "i"}}
        ]
    
    if observer != None:
        query["$or"] = [
            {"plate_info.observer" : { "$regex" : observer, "$options" : "i"}},
        ]

    try:

        results_count = glassplates.count_documents(query)
        plates = (
                (
                    glassplates.find(query)
                        .sort([(sort_order,pymongo.ASCENDING)])
                        .collation({"locale": "en_US", "numericOrdering": True})
                    )
                    .skip(skip)
                    .limit(limit)
                )
        
        plates_out = json.loads(dumps(plates))

        results = {
            "total" : results_count,
            "limit" : limit,
            "skip" : skip,
            "results" : plates_out,
        }
        return results

    except:
        results = {
            "total" : 0,
            "limit" : limit,
            "skip" : skip,
            "results": []
        }
        return results


# show all archives
@app.get("/archives")
def list_archives(skip: int=0, limit: int = 50):

    try:

        results_count = archives.count_documents({})
        archive = (
                (
                    archives.find({})
                        .sort([('identifier',pymongo.ASCENDING)])
                        .collation({"locale": "en_US", "numericOrdering": True})
                    )
                    .skip(skip)
                    .limit(limit)
                )

        archive_out = json.loads(dumps(archive))

        results = {
            "total" : results_count,
            "limit" : limit,
            "skip" : skip,
            "results" : archive_out,
        }
        return results

    except:
        results = {
            "total" : 0,
            "limit" : limit,
            "skip" : skip,
            "results": []
        }
        return results

# show details about one archives
@app.get("/archives/{archive_id}")
def archive_details(archive_id):

    try:
        query = {}
        query["identifier"] = { "$regex" : archive_id, "$options" : "i"}
        
        archive = archives.find(query)
        archive_out = json.loads(dumps(archive))

        results = {
             "results" : archive_out,
        }
        return results

    except:
        results = {
            "results": []
        }
        return results


# show plates in specific archive
@app.get("/{archive_id}")
def List_plates_in_archive(archive_id, skip: int=0, limit: int = 50):
    try:
        query = {}
        query["archive"] = { "$regex" : archive_id, "$options" : "i"}

        results_count = glassplates.count_documents(query)
        plates = (
                (
                    glassplates.find(query)
                        .sort([('identifier',pymongo.ASCENDING)])
                        .collation({"locale": "en_US", "numericOrdering": True})
                    )
                    .skip(skip)
                    .limit(limit)
                )
        
        plates_out = json.loads(dumps(plates))

        results = {
            "total" : results_count,
            "limit" : limit,
            "skip" : skip,
            "results" : plates_out,
        }
        return results

    except:
        results = {
            "total" : 0,
            "limit" : limit,
            "skip" : skip,
            "results": []
        }
        return results


# show one specific plate
@app.get("/{archive_id}/{plate_id}")
def plate_details(archive_id, plate_id):

    try:
        query = {}
        query["archive"] = { "$regex" : archive_id, "$options" : "i"}
        query["identifier"] = { "$regex" : '^'+plate_id+'$', "$options" : "i"}

        plates = json.loads(dumps(glassplates.find_one(query)))
        return plates

    except:
        results = {
            "results": []
        }
        return results




