"""
Provides helper methods to easily mock files in unittests to avoid using the FS. 
"""

from mock import MagicMock as Mock

__RCSID__ = "$Id$"

class FileUtil(object):
  """Provides utility methods to mock files"""

  #Staticmethod since it doesn't require the class, just logically bound
  @staticmethod
  def get_multiple_read_handles( file_contents ):
    """ Utility function to mock multiple read and write calls in a method
    :param list file_contents: List of list of strings

    Usage of this method, both for read() calls and for iteration. WARNING: You cannot mix read() and iteration calls!! (Iteration and read() have separate counters for the files):
    
    file_contents = [['line1file1', 'line2file1'], ['line1file2', 'line2file2']]
    from ILCDIRAC.Workflow.Modules import WhizardAnalysis
    handles = FileUtil.get_multiple_read_handles(file_contents)
    moduleName = "ILCDIRAC.Workflow.Modules.WhizardAnalysis"
    with patch('%s.open' % moduleName, mock_open(), create=True) as mo:
      mo.side_effect = (h for h in handles)
      WhizardAnalysis.runIt()
      
    # Check if files are opened correctly
    mo.assert_any_call('filename', 'mode') #mode is 'r','w', etc.
    # Check if output to files is correct
    expected = [[], ['line100', 'firstlineentry']] # Means 2 files will be opened, nothing is written to first file, and 'firstlineentry' and 'line100' are written (in different calls and exactly these strings) to the second file. If more/less is written this fails!
    self.assertEquals(len(file_contents), len(expected))
    for (index, handle) in enumerate(handles):
      cur_handle = handle.__enter__()
      self.assertEquals(len(expected[index]), handle.__enter__.return_value.write.call_count)
      for entry in expected[index]:
        cur_handle.write.assert_any_call(entry)
    """
    full_file_contents = ['\n'.join(x) for x in file_contents]
    gens = []
    for filecontent in file_contents:
      gens.append((f for f in filecontent))
    amount_of_files = len(gens)
    handles = []
    for i in range(0, amount_of_files):
      curhandle = Mock()
      curhandle.__enter__.return_value.read.side_effect = lambda: full_file_contents.pop(0)
      curhandle.__enter__.return_value.__iter__.return_value = gens[i]
      handles.append(curhandle)
    return handles

  @staticmethod
  def check_file_interactions( testobject, mockobject, expected_tuples, expected_output, handles ):
    """Checks if the actual test interaction with the files matches the expected behaviour.
    :param TestCase testobject: Unit Testcase, used to call assert methods
    :param mock_open mockobject: Mock object that mocks the open() method, used to get call information
    :param list expected_tuples: List of tuples of strings ('filename', 'mode') of ALL expected open() calls
    :param list expected_output: List of list of strings containing everything that is written to any file. expected_output[i] is the expected output of the i-th opened file, expected_output[i][j] is one string that is supposed to be written to the i-th file.
    :param Iterable(Mock) handles: return value of the get_multiple_read_handles method, used to get call information
    """
    # Check if expected open() calls match the actual ones exactly (same amount + each element of expected_tuples is in the calls of the mockobject --- maybe does not work for duplicate calls?)
    testobject.assertEquals(len(expected_tuples), len(mockobject.mock_calls))
    for (filename, filemode) in expected_tuples:
      mockobject.assert_any_call(filename, filemode)
    # Check if expected write() calls match the actual ones exactly (same as above --- think about duplicates #FIXME )
    testobject.assertEquals(len(expected_output), len(handles))
    for (index, handle) in enumerate(handles):
      cur_handle = handle.__enter__()
      testobject.assertEquals(len(expected_output[index]), handle.__enter__.return_value.write.call_count)
      for entry in expected_output[index]:
        cur_handle.write.assert_any_call(entry)
