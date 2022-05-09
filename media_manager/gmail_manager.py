import base64
import os
import pickle
import argparse


# for setting-up, see https://developers.google.com/gmail/api/quickstart/python
# https://www.thepythoncode.com/article/use-gmail-api-in-python


# pip3 install --upgrade google-api-python-client google-auth-httplib2 google-auth-oauthlib
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request


# for encoding/decoding messages in base64
from base64 import urlsafe_b64decode, urlsafe_b64encode

# for dealing with attachement MIME types
from email.mime.multipart import MIMEMultipart
from email.mime.image import MIMEImage
from email.mime.audio import MIMEAudio
from email.mime.base import MIMEBase
from mimetypes import guess_type as guess_mime_type


# Request all access (permission to read/send/receive emails, manage the inbox, and more)
SCOPES = ['https://mail.google.com/']
MY_GMAIL = 'martin.l.ouellet@gmail.com'
MY_CREDENTIAL = os.path.join(os.path.expanduser('~'), '.google', 'credentials.json')
MY_PICKLE_TOKEN = os.path.join(os.path.expanduser('~'), '.google', 'credential_token.pickle')

def format_size(b, suffix='b'):
    for unit in ('', 'K', 'M', 'G', 'T'):
        if b < 1024:
            return f"{b:.2f} {unit}{suffix}"
        b /= 1024
        return f"{b:.2f} P{suffix}"

def clean(text):
    # clean text for creating a folder
    return "".join(c if c.isalnum() else "_" for c in text)
    

def gmail_authenticate():
    """ Loads credentials.json, does the authentication with Gmail API
    and returns a service object 
    """
    creds = None

    #  token file stores user's access and refresh tokens
    #  and is created automatically when authorization flow completes the first time
    if os.path.exists(MY_PICKLE_TOKEN):
        with open(MY_PICKLE_TOKEN, 'rb') as token:
            creds = pickle.load(token)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(MY_CREDENTIAL, SCOPES)
            creds = flow.run_local_server(port=0)
        # save the credentials for the next run
        with open(MY_PICKLE_TOKEN,'wb') as token:
            pickle.dump(creds, token)
    
    return build('gmail', 'v1', credentials=creds)


def search_messages(service, query):
    """Search email and return all paginated messages. Each Message includes their IDs, 
    useful to later process them (delete, mark as read, mark as unread, and search features..)
    """
    print(f"{query}")
    result = service.users().messages().list(userId='me',q=query).execute()
    messages = []
    if 'messages' in result:
        messages.extend(result['messages'])
    while 'nextPageToken' in result:
        page_token = result['nextPageToken']
        result = service.users().messages().list(userId='me',q=query, pageToken=page_token).execute()
        if 'messages' in result:
            messages.extend(result['messages'])
    return messages


def search_with_attachment(service, file_types, after=None, before=None, larger_than=None):
    file_types_s = [f'filename:{f}' for f in file_types]
    search_query = " OR ".join(file_types_s)
    # date format : 'yyyy/mm/dd'
    if after:
        search_query += f' after:{after}'
    if before:
        search_query += f' before:{before}'
    if larger_than:
        search_query += f' larger_than:{larger_than}'

    return search_messages(service, search_query)


def delete_messages(service, query):
    messages_to_delete = search_messages(service, query)
    # it's possible to delete a single message with the delete API, like this:
    # service.users().messages().delete(userId='me', id=msg['id'])
    # but it's also possible to delete all the selected messages with one query, batchDelete
    return service.users().messages().batchDelete(
      userId='me',
      body={
          'ids': [ msg['id'] for msg in messages_to_delete]
      }
    ).execute()


def parse_parts(service, parts, folder_name, message):
    """
    Utility function that parses the content of an email partition
    """
    if parts:
        for part in parts:
            filename = part.get("filename")
            mimeType = part.get("mimeType")
            body = part.get("body")
            data = body.get("data")
            file_size = body.get("size")
            part_headers = part.get("headers")
            if part.get("parts"):
                # recursively call this function when we see that a part
                # has parts inside
                parse_parts(service, part.get("parts"), folder_name, message)
            if mimeType == "text/plain":
                # if the email part is text plain
                if data:
                    text = urlsafe_b64decode(data).decode()
                    print(text)
            elif mimeType == "text/html":
                # if the email part is an HTML content
                # save the HTML file and optionally open it in the browser
                if not filename:
                    filename = "index.html"
                filepath = os.path.join(folder_name, filename)
                print("Saving HTML to", filepath)
                with open(filepath, "wb") as f:
                    f.write(urlsafe_b64decode(data))
            else:
                # attachment other than a plain text or HTML
                for part_header in part_headers:
                    part_header_name = part_header.get("name")
                    part_header_value = part_header.get("value")
                    if part_header_name == "Content-Disposition":
                        if "attachment" in part_header_value:
                            # we get the attachment ID 
                            # and make another request to get the attachment itself
                            print("Saving the file:", filename, "size:", format_size(file_size))
                            attachment_id = body.get("attachmentId")
                            attachment = service.users().messages() \
                                        .attachments().get(id=attachment_id, userId='me', messageId=message['id']).execute()
                            data = attachment.get("data")
                            filepath = os.path.join(folder_name, filename)
                            if data:
                                with open(filepath, "wb") as f:
                                    f.write(urlsafe_b64decode(data))


def read_message(service, message):
    """
    This function takes Gmail API `service` and the given `message_id` and does the following:
        - Downloads the content of the email
        - Prints email basic information (To, From, Subject & Date) and plain/text parts
        - Creates a folder for each email based on the subject
        - Downloads text/html content (if available) and saves it under the folder created as index.html
        - Downloads any file that is attached to the email and saves it in the folder created
    """
    msg = service.users().messages().get(userId='me', id=message['id'], format='full').execute()
    # parts can be the message body, or attachments
    payload = msg['payload']
    headers = payload.get("headers")
    parts = payload.get("parts")
    folder_name = "email"
    has_subject = False
    if headers:
        # this section prints email basic info & creates a folder for the email
        for header in headers:
            name = header.get("name")
            value = header.get("value")
            if name.lower() == 'from':
                # we print the From address
                print("From:", value)
            if name.lower() == "to":
                # we print the To address
                print("To:", value)
            if name.lower() == "subject":
                # make our boolean True, the email has "subject"
                has_subject = True
                # make a directory with the name of the subject
                folder_name = clean(value)
                # we will also handle emails with the same subject name
                folder_counter = 0
                while os.path.isdir(folder_name):
                    folder_counter += 1
                    # we have the same folder name, add a number next to it
                    if folder_name[-1].isdigit() and folder_name[-2] == "_":
                        folder_name = f"{folder_name[:-2]}_{folder_counter}"
                    elif folder_name[-2:].isdigit() and folder_name[-3] == "_":
                        folder_name = f"{folder_name[:-3]}_{folder_counter}"
                    else:
                        folder_name = f"{folder_name}_{folder_counter}"
                os.mkdir(folder_name)
                print("Subject:", value)
            if name.lower() == "date":
                # we print the date when the message was sent
                print("Date:", value)
    if not has_subject:
        # if the email does not have a subject, then make a folder with "email" name
        # since folders are created based on subjects
        if not os.path.isdir(folder_name):
            os.mkdir(folder_name)
  
    parse_parts(service, parts, folder_name, message)
    print("="*50)



def download_attachments(service, user_id, msg_id, store_dir):
    """Get and store attachment from Message with given id.
    service: Authorized Gmail API service instance.
    user_id: User's email address. Use value "me" to indicate authenticated user.
    msg_id: ID of Message containing attachment.
    prefix: prefix added to the attachment filename on saving

    https://stackoverflow.com/questions/25832631/download-attachments-from-gmail-using-gmail-api
    """    
    try:
        message = service.users().messages().get(userId=user_id, id=msg_id).execute()
        parts = [message['payload']]
        while parts:
            part = parts.pop()
            if part.get('parts'):
                parts.extend(part['parts'])
            if part.get('filename'):
                file_data = None
                if 'data' in part['body']:
                    file_data = base64.urlsafe_b64decode(part['body']['data'].encode('UTF-8'))
                    print(f"FileData for {message['id']}, {part['filename']} found! size: {part['size']}")
                elif 'attachmentId' in part['body']:
                    attachment = service.users().messages().attachments().get(
                        userId=user_id, messageId=message['id'], id=part['body']['attachmentId']
                    ).execute()
                    file_data = base64.urlsafe_b64decode(attachment['data'].encode('UTF-8'))
                    print(f"FileData for {message['id']}, {part['filename']} found! size: {attachment['size']}")

                if file_data:
                    filename = f"EmailId-{msg_id}_{part['filename']}"
                    filepath = os.path.join(store_dir, filename)
                    print(f'Saving file={filepath}')
                    with open(filepath, 'wb') as f:
                        f.write(file_data)
    except Exception as error:
        print('An error occurred: %s' % error)


def download_email_attachments():
    pass


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('out_dir', help="Dir where to download files")
    parser.add_argument('years', help="Limit search 'From-To' year")
    parser.add_argument('-t','--file_types', default="jpeg,jpg,bmp,gif")
    parser.add_argument('-lt','--larger_than', default="100k")

    args = parser.parse_args()

    service = gmail_authenticate()

    out_dir = os.path.abspath(args.out_dir)
    if not os.path.exists(out_dir):
        os.makedirs(out_dir)

    if args.years != 'all':
        after_yr, before_yr = args.years.split('-')
        msgs_found = search_with_attachment(service, 
                        file_types=args.file_types.split(','), 
                        after=f'{after_yr}/01/01', 
                        before=f'{before_yr}/01/01',
                        larger_than=args.larger_than)
    else:
        msgs_found = search_with_attachment(service, 
                        file_types=args.file_types.split(','),
                        larger_than=args.larger_than)

    for msg in msgs_found:
        download_attachments(service, user_id='me', msg_id=msg['id'], store_dir=out_dir)


