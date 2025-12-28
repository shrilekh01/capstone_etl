from Configuration.etlconfig import *
import paramiko

class CommonUtilities:

    def sales_data_from_linux_server(self):
        # download the sales file from Linux server to local via SFTP/SSH
        ssh_client = paramiko.SSHClient()
        ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        ssh_client.connect(
            hostname=hostname,
            username=username,
            password=password
        )

        sftp = ssh_client.open_sftp()
        sftp.get(
            remote_file_path,
            local_file_path
        )

        sftp.close()
        ssh_client.close()
