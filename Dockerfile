FROM rust:bookworm

RUN apt update && apt install --no-install-recommends wget -y

WORKDIR /renoir
RUN wget https://dl.min.io/client/mc/release/linux-amd64/mc
RUN chmod +x mc

COPY resources/docker /renoir

ENTRYPOINT [ "./compile.sh" ]