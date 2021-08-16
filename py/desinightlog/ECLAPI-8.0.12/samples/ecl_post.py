from __future__ import print_function
from ECLAPI import ECLConnection, ECLEntry

URL = "http://rexdb03.fnal.gov:8080/ECL/demo"
password = ""
user = ""



if __name__ == '__main__':
    '''
    '''
    # For testing only!
    import ssl
    ssl._create_default_https_context = ssl._create_unverified_context

    import getopt, sys

    opts, args = getopt.getopt(sys.argv[1:], 'np:u:U:')

    print_only = False

    for opt, val in opts:
        if opt=='-n':           # Print the entry to screen, not to post it
            print_only = True
        if opt=='-p':           # User password
            password = val
        if opt=='-u':           # User name
            user = val
        if opt == '-U':         # ECL instance URL to use for the posts
            URL = val


##########################################################################
#   Create test entry
    e = ECLEntry(category='Sandbox',
                 tags=['Muon'],
                 formname='default',
                 text="""This is just a test of automated posting""",
                 preformatted=False)

    if True:
        # Optional. Set the entry comment
        e.addSubject('Simple entry subject, Simple entry subject, Simple entry subject, Simple entry subject')

    if False:
        # Assume the form has the fields named 'firstname', 'lastname', 'email'
        # Fill some fields in the form
        e.setValue(name="firstname", value='John')
        e.setValue(name="lastname", value='Doe')
        e.setValue(name="email", value='johndoe@domain.net')

    if False:
        # Attach some file
        e.addAttachment(name='attached-file', filename='/bin/zcat',
                data='Data may come here as an argument. The file will not be read in this case')

    if False:
        # Attach some image
        # Image data also can be passed as a parameter using 'image' argument.
        e.addImage(name='world-logo', filename='/var/www/icons/world1.png', image=None)

    if not print_only:
        # Define connection
        elconn = ECLConnection(url=URL, username=user, password=password)
        #
        # The user should be a special user created with the "raw password" by administrator.
        # This user cannot login via GUI

        # Post test entry created above
        response = elconn.post(e)

        # Print what we have got back from the server
        print(response)

        # Close the connection
        elconn.close()
    else:
        # Just print prepared XML
        print(e.xshow())

