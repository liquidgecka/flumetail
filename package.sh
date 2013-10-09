#!/bin/bash -ex

PPA=liquidgecka/flume

if [ ! -f flumetail.go ] ; then
  echo "Please run this from the flumetail root." >2
  exit 1
fi

# Get the version details from the changelog.
FULLVERSION=$(head -n1 debian/changelog | egrep -o '\(.*\)' | tr -d '[()]')
VERSION="$(echo "$FULLVERSION" | cut -d- -f1)"
RELEASE="$(echo "$FULLVERSION" | cut -d- -f2)"

# Check to see if the given release already exists in the PPA. If so we can not
# upload so we abort and tell the user to run dch.
URL="http://ppa.launchpad.net/${PPA}/ubuntu/pool/main/f/flumetail/flumetail_${FULLVERSION}.dsc"
if curl -s "${URL}" | egrep -q "^Version: ${FULLVERSION}\$" ; then
  echo "This version already exists in the PPA. Please run dch to update the"
  echo "change log before running this script."
  exit 1
fi

TEMPDIR="$(mktemp -d /tmp/flumetail.XXXXX)"
mkdir -p "${TEMPDIR}/flumetail-${VERSION}"
cp -rf * "${TEMPDIR}/flumetail-${VERSION}/"
cd "${TEMPDIR}"
tar -c "flumetail-${VERSION}" | gzip > "flumetail_${VERSION}.orig.tar.gz"

(cd "${TEMPDIR}/flumetail-${VERSION}/" && debuild -S -sa)
dput ppa:liquidgecka/flume "flumetail_${FULLVERSION}_source.changes"
backportpackage -d quantal --upload "ppa:${PPA}" "flumetail_${FULLVERSION}.dsc"
backportpackage -d precise --upload "ppa:${PPA}" "flumetail_${FULLVERSION}.dsc"
