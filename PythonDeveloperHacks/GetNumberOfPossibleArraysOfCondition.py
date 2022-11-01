# Python program for implementation 
# check criteria
def checkcriteria(arr):
    w = 0
    for i in range(len(arr)):
        if i%arr[i] == 0 or arr[i]%i == 0:
        	w=w+1
    return 1 if w > 0 else 0	
    	
# Find combinations 		
def bubbleSort(arr): 
    matches = 1
    n = len(arr)
    # Traverse through all array elements
    for i in range(n): 
        # Last i elements are already in place
        for j in range(n-i-1): 
            # traverse the array from 0 , n-i-1
            arr[j], arr[j+1] = arr[j+1], arr[j]
            result = checkcriteria(arr)
            if result == 1:
                matches = matches+1
    return matches
                    
#create an array
def createAnArray(m):
    createdarray = []
    if m >1 and m < 20:
        createdarray.extend(iter(range(1, m)))
    else:
        print ("Given number is out of range")
    return createdarray

# Driver code to test above
userintinput = input("Enter a number >1 and <20 to create an array and to find possible arrays condition i%arr[i] == 0 or arr[i]%i == 0 :")
userintinput = int(userintinput)
print(f"Number entered by you is: {userintinput}")
arr = createAnArray(userintinput)
print (arr)
outputvalue = bubbleSort(arr)
print(f"matches found are {outputvalue}")  
