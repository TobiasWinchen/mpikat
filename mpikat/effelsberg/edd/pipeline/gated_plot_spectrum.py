"""
Reads data from stdin and writes a base64 encoded png to stdout

ToDo; add arguments to test plotting interactively


data dict: [key] [list of data sets]
plotmap dict: [key] figure numer
"""
import matplotlib as mpl
mpl.use('Agg')
mpl.rcParams.update(mpl.rcParamsDefault)
import numpy as np
import pylab as plt
import astropy.time

import sys
import cPickle as pickle
if sys.version_info[0] >=3:
    import io
else:
    import cStringIO as io
    io.BytesIO = io.StringIO

input_data = pickle.loads(sys.stdin.read())
data = input_data['data']
plotmap = input_data['plotmap']
ndisplay_channels = 1024

import base64
########################################################################
# Actual plot routine
########################################################################
# Reorder by subplot
ko = {}
for k,i in plotmap.items():
    if i not in ko:
        ko[i] = []
    ko[i].append(k)

figsize = {2: (8, 4), 4: (8, 8)}
fig = plt.figure(figsize=figsize[len(ko)])

timestamp = None

# loop over suplotindices
for si, plot_keys in ko.items():
    sub = fig.add_subplot(si)

    if not data:
        sub.text(.5, .5, "NO DATA")

    for k in sorted(plot_keys):
        S = np.zeros(ndisplay_channels)
        T = 1E-64

        if k not in data:
            #_log.warning("{} not in data!".format(k))
            #_log.debug("Data: {}".format(data))
            continue

        for d in data[k]:
            di = d['spectrum'][1:].reshape([ndisplay_channels, (d['spectrum'].size -1) // ndisplay_channels]).sum(axis=1)
            if np.isfinite(di).all():
                S += di
                T += d['integration_time']
                timestamp = d['timestamp'][-1]

        sub.plot(10. * np.log10(S / T), label="{} (T = {:.2f} s)".format(k.replace('_', ' '), T))
    sub.legend()
    sub.set_xlabel('Channel')
    sub.set_ylabel('PSd [dB]')
if timestamp:
    fig.suptitle('{}'.format(astropy.time.Time(timestamp, format='unix').isot))
else:
    fig.suptitle('NO DATA {}'.format(astropy.time.Time.now().isot))

fig.tight_layout(rect=[0, 0.03, 1, 0.95])

########################################################################
# End of plot
########################################################################
fig_buffer = io.BytesIO()
fig.savefig(fig_buffer, format='png')
fig_buffer.seek(0)
b64 = base64.b64encode(fig_buffer.read())

sys.stdout.write(b64)



