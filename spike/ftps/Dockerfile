FROM debian:bookworm-slim

# Instalar dependências e pure-ftpd
RUN apt-get update && apt-get -y install \
    pure-ftpd \
    pure-ftpd-common \
    openssl \
    python3 \
    python3-pip \
    python3-venv \
    git \
    docker.io \
    && rm -rf /var/lib/apt/lists/*

# Criar diretórios necessários
RUN mkdir -p /etc/ssl/private /etc/pure-ftpd/passwd /home/ftpusers/ftptest

# Criar usuário ftpuser
RUN useradd -r -d /var/lib/ftp -s /bin/false ftpuser

# Gerar certificados SSL
RUN openssl dhparam -out /etc/ssl/private/pure-ftpd-dhparams.pem 2048
RUN openssl req -x509 -nodes -newkey rsa:2048 -sha256 -keyout /etc/ssl/private/pure-ftpd.pem -out /etc/ssl/private/pure-ftpd.pem -subj "/C=UK/ST=Warwickshire/L=Leamington/O=OrgName/OU=IT Department/CN=example.com"
RUN chmod 600 /etc/ssl/private/*.pem

# Criar usuário FTP
RUN (echo ftptest; echo ftptest) | pure-pw useradd ftptest -f /etc/pure-ftpd/passwd/pureftpd.passwd -m -u ftpuser -d /home/ftpusers/ftptest

# Configurar TLS no pure-ftpd
RUN echo "TLS 1" >> /etc/pure-ftpd/conf/TLS && \
    echo "/etc/ssl/private/pure-ftpd.pem" > /etc/pure-ftpd/conf/TLSCertificateFile

# Script de inicialização
RUN echo '#!/bin/bash\n\
set -e\n\
PUBLICHOST=${PUBLICHOST:-localhost}\n\
echo "Regenerating password database..."\n\
pure-pw mkdb /etc/pure-ftpd/pureftpd.pdb -f /etc/pure-ftpd/passwd/pureftpd.passwd\n\
echo "Starting pure-ftpd on $PUBLICHOST..."\n\
exec /usr/sbin/pure-ftpd -c 30 -C 10 -l puredb:/etc/pure-ftpd/pureftpd.pdb -E -j -R -P $PUBLICHOST -p 30000:30059' > /run.sh && \
    chmod +x /run.sh

CMD ["/run.sh"]
