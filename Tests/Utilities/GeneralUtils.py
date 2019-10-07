"""Contains general utility methods for unit testing.
"""

from __future__ import print_function
import os
import pwd

__RCSID__ = "$Id$"

def assertEqualsImproved( val1, val2, assertobject ):
  """Asserts that val1 and val2 are equal and automatically generates a meaningful error message. Only disadvantage is that you have to check 1 method up in the stacktrace.

  :param T val1: First value to be compared
  :param T val2: Second value, compared to val1
  :param TestCase assertobject: Testcase object, used to gain the base assertEquals method.
  """
  assertobject.assertEquals( val1, val2, "Test expected these values to be the same, but they were not!\n First value    = %s,\n Second value   = %s" % (repr(val1), repr(val2)) )

def assertInImproved( val1, val2, assertobject ):
  """Asserts that val1 is contained in val2 and automatically generates a meaningful error message. Only disadvantage is that you have to check 1 method up in the stacktrace.

  :param T val1: First value
  :param S val2: Second value, can contain val1
  :param TestCase assertobject: Testcase object, used to gain the base assertEquals method.
  """
  assertobject.assertIn( val1, val2, "Test expected these values to be the same, but they were not!\n First value = %s,\n Second value = %s" % (repr(val1), repr(val2)) )

def assertEqualsXml( elem1, elem2, assertobject ):
  """Asserts that the two passed XMLTree Elements are equal, i.e. have the same content, name, etc.
  """
  assertEqualsImproved( elem1.tag, elem2.tag, assertobject )
  assertEqualsImproved( elem1.text, elem2.text, assertobject )
  assertEqualsImproved( elem1.tail, elem2.tail, assertobject )
  assertEqualsImproved( elem1.attrib, elem2.attrib, assertobject )

def assertEqualsXmlTree( root1, root2, assertobject ):
  """ Asserts that the two passed XML trees and all their contained elements are equal.
  """
  print('root1 = %s, root2 = %s' % (root1, root2))
  print('kinder1 = %s, kinder2 = %s' % (list(root1), list(root2)))
  assertEqualsXml( root1, root2, assertobject )
  children1 = list( root1 )
  assertEqualsImproved( len( children1 ), len( list(root2) ), assertobject )
  for child1 in children1:
    child2 = root2.find( child1.tag )
    assert child2 is not None
    assertEqualsXmlTree( child1, child2, assertobject )

def assertListContentEquals( list1, list2, assertobject ):
  """Asserts that two lists contain the same elements, regardless of order, else a useful debug message is returned
  Checks if both list have the same length first, then checks if each element of one list  is contained in the
  other list. Duplicates have to be in the lists the same amount of times.
  """
  assertEqualsImproved( len(list1), len(list2), assertobject )
  # Code similar to assertMockCalls but can't reuse
  tmp_compare_list = list( list2 ) # Copies the references of the second list (shallow copy)
  for elem1 in list1:
    try:
      tmp_compare_list.remove( elem1 )
    except ValueError as v_err:
      assertobject.fail( 'The two passed lists do not contain the same elements.\n %s was not found (often enough) in the second list. Original lists: \n %s \n %s \n Error: %s' % ( elem1, list1, list2, v_err ) )

  assertobject.assertFalse( tmp_compare_list, 'The two passed lists do not contain the same elements. The following elements from the second list are not contained (often enough) in the first list: %s\n Original lists: \n %s \n %s \n ' % ( tmp_compare_list, list1, list2 ) )

def assertDiracFails( result, assertobject ):
  """Asserts that result, which is the return value of a dirac method call, is an S_ERROR.

  :param dict result: Structure (expected to be S_ERROR) returned by the dirac call
  :param TestCase assertobject: Testcase object, used to gain the assertX methods.
  """
  assertobject.assertFalse( result['OK'] )

def assertDiracFailsWith( result, errorstring, assertobject ):
  """Asserts that result, which is the return value of a dirac method call, is an S_ERROR with errorstring contained in the error message (case insensitive).

  :param dict result: Structure (expected to be S_ERROR) returned by the dirac call
  :param str errorstring: String expected to be contained in the error message
  :param TestCase assertobject: Testcase object, used to gain the assertX methods.
  """
  assertobject.assertFalse( result['OK'] )
  assertobject.assertIn( errorstring.lower(), result['Message'].lower() )

def assertDiracFailsWith_equals( result, retval, assertobject ):
  """Asserts that result, which is the return value of a dirac method call, is an S_ERROR with the retval object.

  :param dict result: Structure (expected to be S_ERROR) returned by the dirac call
  :param object errorstring: Object expected to be contained in the error message
  :param TestCase assertobject: Testcase object, used to gain the assertX methods.
  """
  assertobject.assertFalse( result['OK'] )
  assertEqualsImproved( retval, result['Message'], assertobject )

def assertDiracSucceeds( result, assertobject ):
  """Asserts that result, which is the return value of a dirac method call, is an S_OK, else print out the error message.

  :param dict result: Structure (expected to be S_OK) returned by the dirac call
  :param TestCase assertobject: Testcase object, used to gain the assertX methods.
  """
  assertobject.assertTrue( result['OK'], result.get('Message', ''))

def assertDiracSucceedsWith( result, expected_val, assertobject ):
  """Asserts that result, which is the return value of a dirac method call, is an S_OK with expected_val in the returned value.

  :param dict result: Structure (expected to be S_OK) returned by the dirac call
  :param ? expected_val: Part of the value in the S_OK structure (checked with `in`)
  :param TestCase assertobject: Testcase object, used to gain the assertX methods.
  """
  assertDiracSucceeds( result, assertobject )
  assertobject.assertIn( expected_val, result['Value'] )

def assertDiracSucceedsWith_equals( result, expected_res, assertobject ):
  """Asserts that result, which is the return value of a dirac method call, is an S_OK containing exactly expected_res.

  :param dict result: Structure (expected to be S_OK) returned by the dirac call
  :param ? expected_res: Value that should be contained in the result.
  :param TestCase assertobject: Testcase object, used to gain the assertX methods.
  """
  assertDiracSucceeds( result, assertobject )
  assertobject.assertEquals( expected_res, result['Value'] )

def assertMockCalls( mock_object_method, argslist, assertobject, only_these_calls = True ):
  """ Asserts that the passed mocked method has been called with the arguments provided in argslist, in any order.

  :param Mock mock_object_method: Method of a mock object that is under test
  :param argslist: list of the expected arguments for all calls to the mocked method. Tuples are unpacked and represent multiple arguments
  :type argslist: `python:list`
  :param TestCase assertobject: The TestCase instance running the tests, in order to gain access to the assertion methods
  :param bool only_these_calls: Indicates what happens if the calls in argslist is a strict subset of the actual call list. True means the assertion fails, False means the assertion holds.
  """
  from mock import call
  mock_call_list = list( mock_object_method.mock_calls ) # Creates a copy of the mock_calls list with references to the original elements (shallow copy), a bit faster than copy.copy( mock_calls )
  call_list = []
  for args in argslist:
    if isinstance( args, tuple ):
      call_list.append( call( *args ) )
    else:
      call_list.append( call( args ) )

  for expected_call in call_list:
    try:
      mock_call_list.remove( expected_call )
    except ValueError:
      assertobject.fail( 'Expected the mock to be called with the passed arglist but that was not the case for the call %s\n List of expected calls: %s \n List of actual calls: %s' % ( expected_call, argslist, mock_call_list ) )
  # TODO test these two new methods, find way to handle positional arguments, beautify output
  if only_these_calls:
    assertobject.assertFalse( mock_call_list, "The following calls were made on the mock object but don't have a respective entry in the argslist: %s" % mock_call_list )

def assertMockCalls_ordered( mock_object_method, argslist, assertobject ):
  """ Asserts that the passed mocked method has been called with the arguments provided in argslist (and only those arguments), in exactly the given order.

  :param Mock mock_object_method: Method of a mock object that is under test
  :param argslist: list of the expected arguments for all calls to the mocked method. Tuples are unpacked and represent multiple arguments
  :type argslist: `python:list`
  """
  from mock import call
  call_list = []
  for args in argslist:
    if isinstance( args, tuple ):
      call_list.append( call( *args ) )
    else:
      call_list.append( call( args ) )
  assertEqualsImproved( mock_object_method.mock_calls, call_list, assertobject )


def assertDictEquals(actual_dict, expected_dict, assertobject):
  """ Asserts that the two passed dictionaries contain the same key-value-pairs.

  :param dict actual_dict: Dictionary returned by the method under test
  :param dict expected_dict: Dictionary with the expected key-value-pairs
  :param TestCase assertobject: The current test, used to gain the assertion methods.
  :returns: None, AssertionError if they are not equivalent
  :rtype: None
  """
  assertEqualsImproved(len(actual_dict), len(expected_dict), assertobject)
  for key in actual_dict:
    assertEqualsImproved(actual_dict[key], expected_dict[key], assertobject)


def assertDictEquals_dynamic(actual_dict, expected_dict, assertobject, equalsfunc):
  """ Asserts that the two passed dictionaries contain the same key-value-pairs, using the passed
  equalsfunc to check that.

  :param dict actual_dict: Dictionary produced by the actual call on the code under test in the test case.
  :param dict expected_dict: Dictionary with the expected values.
  :param TestCase assertobject: The testcase object, used to gain the assert methods
  :param function equalsfunc: Function used to determine if two values in the dictionary are the same or not.
  :returns: None, AssertionError if they are not equivalent
  :rtype: None
  """
  assertEqualsImproved(len(actual_dict), len(expected_dict), assertobject)
  for key in actual_dict:
    equalsfunc(actual_dict[key], expected_dict[key], assertobject)

def running_on_docker():
  """ Returns whether the code is currently being executed in a docker VM or on a local (dev) machine.
  This is achieved by checking wether /home/<currently logged in user> exists.
  """
  uid = os.getuid()
  user_info = pwd.getpwuid( uid )
  homedir = os.path.join( os.sep + 'home', user_info.pw_name )
  if os.path.exists( homedir ) and not os.environ.get( 'CI', False ):
    return False
  else:
    return True

class MatchStringWith(str):
  """ helper class to match sub strings in a mock.assert_called_with

  >>> myMock.log.error.assert_called_with( MatchStringWith('error mess') )
  """

  def __eq__(self, other):
    return self in str(other)
