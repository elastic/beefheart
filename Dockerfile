# Based on:
# https://medium.com/permutive/optimized-docker-builds-for-haskell-76a9808eb10b

# First, cache a multi-step image that holds all our dependencies.
# ------------------------------------------------------------------------------
FROM fpco/stack-build:lts-14.3 as dependencies
RUN mkdir -p /usr/local/src
WORKDIR /usr/local

# GHC dynamically links its compilation targets to lib gmp
RUN apt-get update \
  && apt-get download libgmp10
RUN mv libgmp*.deb libgmp.deb

# Docker build should not use cached layer if any of these is modified
WORKDIR /usr/local/src
COPY stack.yaml package.yaml stack.yaml.lock /usr/local/src/
RUN stack build --system-ghc --dependencies-only

# Second, build the project using deps from previous image.
# ------------------------------------------------------------------------------
FROM fpco/stack-build:lts-14.3 as build

# Copy compiled dependencies from previous stage
COPY --from=dependencies /root/.stack /root/.stack
COPY . /usr/local/src

WORKDIR /usr/local/src
RUN stack build --system-ghc
RUN mv "$(stack path --local-install-root --system-ghc)/bin/beefheart" /usr/local/bin/
# Move EKG assets into position, otherwise instrumentation dashboard doesn't work
RUN mkdir -p /usr/local/share/beefheart
RUN find "$(stack path --snapshot-install-root)" -name assets -exec cp -a {} /usr/local/share/beefheart/assets ';'

# Finally, just copy over the bare executable into a smaller image.
# ------------------------------------------------------------------------------
FROM ubuntu:18.04 as app

COPY --from=dependencies /usr/local/libgmp.deb /tmp
RUN dpkg -i /tmp/libgmp.deb && rm /tmp/libgmp.deb

RUN apt update \
  && apt install -y ca-certificates \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

COPY --from=build /usr/local/bin/beefheart /usr/local/bin/
COPY --from=build /usr/local/share/beefheart /usr/local/share/beefheart
ENV ekg_datadir /usr/local/share/beefheart
EXPOSE 8000
CMD ["/usr/local/bin/beefheart"]
