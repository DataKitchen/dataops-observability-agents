# https://learn.microsoft.com/en-us/sql/connect/odbc/linux-mac/installing-the-microsoft-odbc-driver-for-sql-server?view=sql-server-ver16&tabs=alpine18-install%2Calpine17-install%2Cdebian8-install%2Credhat7-13-install%2Crhel7-offline

case $(uname -m) in
    x86_64)   architecture="amd64" ;;
    arm64)   architecture="arm64" ;;
    *) architecture="unsupported" ;;
esac
if [[ "unsupported" == "$architecture" ]];
then
    echo "Alpine architecture $(uname -m) is not currently supported.";
    exit;
fi

#Download the desired package(s)
curl -O https://download.microsoft.com/download/fae28b9a-d880-42fd-9b98-d779f0fdd77f/msodbcsql18_18.5.1.1-1_$architecture.apk
curl -O https://download.microsoft.com/download/7/6/d/76de322a-d860-4894-9945-f0cc5d6a45f8/mssql-tools18_18.4.1.1-1_$architecture.apk

#(Optional) Verify signature, if 'gpg' is missing install it using 'apk add gnupg':
curl -O https://download.microsoft.com/download/fae28b9a-d880-42fd-9b98-d779f0fdd77f/msodbcsql18_18.5.1.1-1_$architecture.sig
curl -O https://download.microsoft.com/download/7/6/d/76de322a-d860-4894-9945-f0cc5d6a45f8/mssql-tools18_18.4.1.1-1_$architecture.sig

curl https://packages.microsoft.com/keys/microsoft.asc  | gpg --import -
gpg --verify msodbcsql18_18.5.1.1-1_$architecture.sig msodbcsql18_18.5.1.1-1_$architecture.apk
gpg --verify mssql-tools18_18.4.1.1-1_$architecture.sig mssql-tools18_18.4.1.1-1_$architecture.apk

#Install the package(s)
apk add --allow-untrusted msodbcsql18_18.5.1.1-1_$architecture.apk
apk add --allow-untrusted mssql-tools18_18.4.1.1-1_$architecture.apk
