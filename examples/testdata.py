from bson.objectid import ObjectId
from examples.models import User, Address, Things, TypelessAddress, TypelessUser
from mongoengine.connection import connect

def add_dataset1():
    address = Address(street="123 Main St")
    address.save()
    address.reload()
    typeless_address = TypelessAddress(street="123 Main St")
    typeless_address.save()
    typeless_address.reload()

    for i in range(0, 100000):
        user1 = User(address_ref=address,address_id=address.id)
        user1.save(safe=False)
        user2 = TypelessUser(address_id=address.id,
                     typeless_address=typeless_address)
        user2.save(safe=False)

connect('examples1')
add_dataset1()

def add_dataset2():

    for i in range(0, 100000):
        thing = Things(long_field="http://www.somelongurl.com?foo=bar&id=%s" % ObjectId())
        thing.save(safe=False)

connect('examples2')
add_dataset2()
