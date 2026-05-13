Seattle's street data is LineStrings, one per block. There is no
intersections table. 

An intersection is where two or more streets meet at an endpoint.
So:

1. Pull both endpoints out of every street segment.
2. Round the coordinates to about 11 cm.
3. Group points that share the same rounded spot.
4. Keep groups with 3 or more segments. Two is just a road
   continuing.
5. Take the centroid of each group. That is the intersection.

Why the centroid
----------------
Each segment was drawn on its own, so the endpoints at the same
corner drift by a few millimeters. Picking one segment's point
would bias the location. The centroid averages them, so I get one
 lat/lon per intersection to join the crashes to.
