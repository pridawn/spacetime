'''
Created on Apr 28, 2016

@author: Arthur Valadares
'''
from __future__ import absolute_import

from rtypes.pcc.types.set import pcc_set
from rtypes.pcc.attributes import primarykey, dimension
from rtypes.pcc.types.projection import projection
from rtypes.pcc.types.subset import subset
from rtypes.pcc.types.join import join
from rtypes.pcc.types.parameter import parameter
import random
import math
import uuid

class Vector3(object):
    def __init__(self, X=0.0, Y=0.0, Z=0.0):
        self.X = X
        self.Y = Y
        self.Z = Z

    # -----------------------------------------------------------------
    def VectorDistanceSquared(self, other) :
        dx = self.X - other.X
        dy = self.Y - other.Y
        dz = self.Z - other.Z
        return dx * dx + dy * dy + dz * dz

    # -----------------------------------------------------------------
    def VectorDistance(self, other) :
        return math.sqrt(self.VectorDistanceSquared(other))

    # -----------------------------------------------------------------
    def Length(self) :
        return math.sqrt(self.VectorDistanceSquared(ZeroVector))

    # -----------------------------------------------------------------
    def LengthSquared(self) :
        return self.VectorDistanceSquared(ZeroVector)

    def AddVector(self, other) :
        return Vector3(self.X + other.X, self.Y + other.Y, self.Z + other.Z)

    # -----------------------------------------------------------------
    def SubVector(self, other) :
        return Vector3(self.X - other.X, self.Y - other.Y, self.Z - other.Z)

    # -----------------------------------------------------------------
    def ScaleConstant(self, factor) :
        return Vector3(self.X * factor, self.Y * factor, self.Z * factor)

    # -----------------------------------------------------------------
    def ScaleVector(self, scale) :
        return Vector3(self.X * scale.X, self.Y * scale.Y, self.Z * scale.Z)

    def ToList(self):
        return [self.X, self.Y, self.Z]

    def Rotate(self, rad):
        heading = math.atan(self.Y/self.X)
        return Vector3()

    # -----------------------------------------------------------------
    def Equals(self, other) :
        if isinstance(other, Vector3):
            return self.X == other.X and self.Y == other.Y and self.Z == other.Z
        elif isinstance(other, tuple) or isinstance(other, list):
            return (
                other[0] == self.X
                and other[1] == self.Y and other[2] == self.Z)

    # -----------------------------------------------------------------
    def ApproxEquals(self, other, tolerance) :
        return self.VectorDistanceSquared(other) < (tolerance * tolerance)

    def __json__(self):
        return self.__dict__

    def __str__(self):
        return self.__dict__.__str__()

    def __eq__(self, other):
        return self.Equals(other)

    def __ne__(self, other):
        return not self.__eq__(other)

    # -----------------------------------------------------------------
    def __add__(self, other) :
        return self.AddVector(other)

    # -----------------------------------------------------------------
    def __sub__(self, other) :
        return self.SubVector(other)

    # -----------------------------------------------------------------
    def __mul__(self, factor) :
        return self.ScaleConstant(factor)

    # -----------------------------------------------------------------
    def __div__(self, factor) :
        return self.ScaleConstant(1.0 / factor)

    @staticmethod
    def __decode__(dic):
        return Vector3(dic['X'], dic['Y'], dic['Z'])

ZeroVector = Vector3()


@pcc_set
class NullSet(object):
    @primarykey(str)
    def ID(self):
        return self._ID

    @ID.setter
    def ID(self, value):
        self._ID = value

@pcc_set
class BaseSet(object):
    @primarykey(str)
    def ID(self):
        return self._ID

    @ID.setter
    def ID(self, value):
        self._ID = value

    @dimension(str)
    def Name(self):
        return self._Name

    @Name.setter
    def Name(self, value):
        self._Name = value

    @dimension(int)
    def Number(self):
        return self._Number

    @Number.setter
    def Number(self, value):
        self._Number = value

    @dimension(list)
    def List(self):
        return self._List

    @List.setter
    def List(self, value):
        self._List = value

    @dimension(dict)
    def Dictionary(self):
        return self._Dictionary

    @Dictionary.setter
    def Dictionary(self, value):
        self._Dictionary = value

    #@dimension(set)
    #def Set(self):
    #    return self._Set

    #@Set.setter
    #def Set(self, value):
    #    self._Set = value

    @dimension(str)
    def Property1(self):
        return self._Property1

    @Property1.setter
    def Property1(self, value):
        self._Property1 = value

    @dimension(str)
    def Property2(self):
        return self._Property2

    @Property2.setter
    def Property2(self, value):
        self._Property2 = value

    @dimension(str)
    def Property3(self):
        return self._Property3

    @Property3.setter
    def Property3(self, value):
        self._Property3 = value

    @dimension(str)
    def Property4(self):
        return self._Property4

    @Property4.setter
    def Property4(self, value):
        self._Property4 = value

    @dimension(str)
    def Property5(self):
        return self._Property5

    @Property5.setter
    def Property5(self, value):
        self._Property5 = value

    @dimension(str)
    def Property6(self):
        return self._Property6

    @Property6.setter
    def Property6(self, value):
        self._Property6 = value

    def __init__(self, num):
        self.ID = str(uuid.uuid4())
        self.Name = ""
        self.Number = num
        self.List = [i for i in xrange(20)]
        #self.Set = set([i for i in xrange(20)])
        self.Dictionary = { str(k) : k for k in xrange(20)}
        self.Property1 = "Property 1"
        self.Property2 = "Property 2"
        self.Property3 = "Property 3"
        self.Property4 = "Property 4"
        self.Property5 = "Property 5"


@projection(BaseSet, BaseSet.ID, BaseSet.Name)
class BaseSetProjection(object):
    @property
    def DecoratedName(self):
        return "** " + self.Name + " **"


@subset(BaseSet)
class SubsetHalf(BaseSet):
    @staticmethod
    def __predicate__(o):
        return o.Number % 2 == 0

@subset(BaseSet)
class SubsetAll(BaseSet):
    @staticmethod
    def __predicate__(o):
        return True

@join(BaseSet, BaseSet)
class JoinHalf(object):

    @primarykey(str)
    def ID(self):
        return self._ID

    @ID.setter
    def ID(self, value):
        self._ID = value

    @dimension(BaseSet)
    def b1(self):
        return self._b1

    @b1.setter
    def b1(self, value):
        self._b1 = value

    @dimension(BaseSet)
    def b2(self):
        return self._b2

    @b2.setter
    def b2(self, value):
        self._b2 = value

    def __init__(self, b1, b2):
        self.b1 = b1
        self.b2 = b2

    def __init__(self, b1, b2):
        self.b1 = b1
        self.b2 = b2

    @staticmethod
    def __predicate__(b1, b2):
        return b1.Number % 2 == 0 and b1.ID == b2.ID

@join(BaseSet, BaseSet)
class JoinAll(object):

    @primarykey(str)
    def ID(self):
        return self._ID

    @ID.setter
    def ID(self, value):
        self._ID = value

    @dimension(BaseSet)
    def b1(self):
        return self._b1

    @b1.setter
    def b1(self, value):
        self._b1 = value

    @dimension(BaseSet)
    def b2(self):
        return self._b2

    @b2.setter
    def b2(self, value):
        self._b2 = value

    def __init__(self, b1, b2):
        self.b1 = b1
        self.b2 = b2

    def __init__(self, b1, b2):
        self.b1 = b1
        self.b2 = b2

    @staticmethod
    def __predicate__(b1, b2):
        return b1.ID == b2.ID

@parameter(BaseSet)
@subset(BaseSet)
class ParameterHalf(BaseSet):
    @staticmethod
    def __predicate__(b1, b2s):
        return b1.Number % 2 == 0

@parameter(BaseSet)
@subset(BaseSet)
class ParameterAll(BaseSet):
    @staticmethod
    def __predicate__(b1, b2s):
        return True
