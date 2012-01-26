from mongoengine import *

class Address(Document):
    street = StringField()

class TypelessAddress(Document):
    meta = {"index_types" : False}
    street = StringField()

class User(Document):
    meta = {
            "indexes": [("address_ref"),
                        ("address_id")
                        ]}

    address_ref = ReferenceField("Address")
    address_id = ObjectIdField()

class TypelessUser(Document):
    meta = {
            "indexes": [("address_id"),
                        ("typeless_address_ref")
                        ],
            "index_types" : False}
    address_id = ObjectIdField()
    typeless_address_ref = ReferenceField("TypelessAddress")


class Things(Document):
    long_field = StringField()