FFT transform bands storage and serving
---
Use FFT or some othre frequency break down method to store different bands for monitored "signal" in the storage.
Then you only serve low pass of the bands up to required resolution and reconstruct signal from them.
Then management of stored resolution can be done by deletion of storage chunks for higest  bands.
See https://kst-plot.kde.org/
