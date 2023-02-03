# -*- coding: utf-8 -*-
"""
@author: Monali Sahu
"""

'''
DESCRIPTION-
    This is a part of utilities package and helps to validate input and output and take appropriate actions.
'''

from pathlib import Path


def input_check(input_file_path):
    
    '''
    ARGUMENT:
        input_file_path: dictionary with dataset names as key and paths for each as value
    RETURNS: boolean value 
        True: if all files present
        False: if any file is not present
    '''
    
    #Loading all input dataset paths
    charges = Path(input_file_path['Charges'])
    endorse = Path(input_file_path['Endorsements'])
    damages = Path(input_file_path['Damages'])
    unit = Path(input_file_path['Unit'])
    primary_person = Path(input_file_path['Primary_Person'])
    restrict = Path(input_file_path['Restrict'])
    
    passed=0
    
    #checking whether the file path passed exist or not
    if (charges.is_file()):
        print("DATA CHECK: Charges - Passed ")
        passed+=1
    else:
        print("DATA CHECK: Charges - Failed ")
    
    if (endorse.is_file()):
        print("DATA CHECK: Endorsements - Passed ")
        passed+=1
    else:
        print("DATA CHECK: Endorsements - Failed ")
        
    if (damages.is_file()):
        print("DATA CHECK: Damages - Passed ")
        passed+=1
    else:
        print("DATA CHECK: Damages - Failed ")
        
    if (unit.is_file()):
        print("DATA CHECK: Unit - Passed ")
        passed+=1
    else:
        print("DATA CHECK: Unit - Failed ")
        
    if (primary_person.is_file()):
        print("DATA CHECK: Primary Person - Passed ")
        passed+=1
    else:
        print("DATA CHECK: Primary Person - Failed ")
        
    if (restrict.is_file()):
        print("DATA CHECK: Restrict - Passed ")
        passed+=1
    else:
        print("DATA CHECK: Restrict - Failed ")
    
    if(passed == 6):
        return True
    else:
        return False

def output_check(output_file_path):
    
    '''
    ARGUMENT:
        output_file_path: dictionary with analysis<n> as key and paths for each as value
    RETURNS: boolean value 
        True: if all paths are provided
        False: if any path is not provided
    '''
    
    #checking whether all the output paths are mentioned
    analysis1=output_file_path.get("Analysis1",False)
    analysis2=output_file_path.get("Analysis2",False)
    analysis3=output_file_path.get("Analysis3",False)
    analysis4=output_file_path.get("Analysis4",False)
    analysis5=output_file_path.get("Analysis5",False)
    analysis6=output_file_path.get("Analysis6",False)
    analysis7=output_file_path.get("Analysis7",False)
    analysis8=output_file_path.get("Analysis8",False)
    
    passed=0
    
    if analysis1:
        print("OUTPUT FILE PATH CHECK: Analysis1 - Present")
        passed+=1
    else:
        print("OUTPUT FILE PATH CHECK: Analysis1 - Missing")
        
    if analysis2:
        print("OUTPUT FILE PATH CHECK: Analysis2 - Present")
        passed+=1
    else:
        print("OUTPUT FILE PATH CHECK: Analysis2 - Missing")
        
    if analysis3:
        print("OUTPUT FILE PATH CHECK: Analysis3 - Present")
        passed+=1
    else:
        print("OUTPUT FILE PATH CHECK: Analysis3 - Missing")
        
    if analysis4:
        print("OUTPUT FILE PATH CHECK: Analysis4 - Present")
        passed+=1
    else:
        print("OUTPUT FILE PATH CHECK: Analysis4 - Missing")
        
    if analysis5:
        print("OUTPUT FILE PATH CHECK: Analysis5 - Present")
        passed+=1
    else:
        print("OUTPUT FILE PATH CHECK: Analysis5 - Missing")
        
    if analysis6:
        print("OUTPUT FILE PATH CHECK: Analysis6 - Present")
        passed+=1
    else:
        print("OUTPUT FILE PATH CHECK: Analysis6 - Missing")
        
    if analysis7:
        print("OUTPUT FILE PATH CHECK: Analysis7 - Present")
        passed+=1
    else:
        print("OUTPUT FILE PATH CHECK: Analysis7 - Missing")
        
    if analysis8:
        print("OUTPUT FILE PATH CHECK: Analysis8 - Present")
        passed+=1
    else:
        print("OUTPUT FILE PATH CHECK: Analysis8 - Missing")
        
    if(passed == 8):
        return True
    else:
        return False

