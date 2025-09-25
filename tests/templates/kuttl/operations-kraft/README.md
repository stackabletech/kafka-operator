
Tests Kraft cluster operations:

- Cluster stop/pause/restart
- Scale brokers up/down
- Scale controllers up/down

TODO:

- Scaling controllers from 3 -> 1 doesn't work.
  Both brokers and controllers try to communicate with old controllers.
  This is why, the last step scales from 5 -> 3 controllers.
  This at least, leaves the cluster in a working state.
