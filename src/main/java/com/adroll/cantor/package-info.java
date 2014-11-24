/**
   Cantor provides utilities for estimating the cardinality
   of large sets.
   <p>
   The algorithms herein are parallelizable, and a Hadoop
   wrapper class is provided for convenience.
   <p>
   It employs most of the HyperLogLog++ algorithm as seen in 
   <a href="http://research.google.com/pubs/pub40671.html">
   this paper</a>, excluding the sparse scheme, and using
   a simple linear interpolation instead of kNN. In addition, 
   it can use MinHash structures to estimate cardinalities of 
   intersections of these sets, as described in 
   <a href="http://tech.adroll.com/blog/data/2013/07/10/hll-minhash.html">
   this blog post</a>.
   <p>
   Both HyperLogLog and MinHash require a precision
   parameter. Basic guidelines are available as follows,
   and {@link com.adroll.cantor.HLLCounter#MIN_P}<code> = 4 &lt;= p &lt;= 18 = </code>
   {@link com.adroll.cantor.HLLCounter#MAX_P}.
   <p>
   <table style="width:auto">
     <tr>
       <td>
         <table style="width:auto">
           <tr> <td colspan="2" class="strong">HyperLogLog <code>p</code> @ 99.7% Confidence</td> </tr>
           <tr class="strong"> <td>p</td> <td>Relative Error</td> </tr>
           <tr style="background-color:#eeeeee"> <td>4</td>  <td>75%</td>  </tr>
           <tr style="background-color:#ffffff"> <td>5</td>  <td>65%</td>  </tr>
           <tr style="background-color:#eeeeee"> <td>6</td>  <td>47%</td>  </tr>
           <tr style="background-color:#ffffff"> <td>7</td>  <td>32%</td>  </tr>
           <tr style="background-color:#eeeeee"> <td>8</td>  <td>23%</td>  </tr>
           <tr style="background-color:#ffffff"> <td>9</td>  <td>16%</td>  </tr>
           <tr style="background-color:#eeeeee"> <td>10</td> <td>10%</td>  </tr>
           <tr style="background-color:#ffffff"> <td>11</td> <td>8%</td>   </tr>
           <tr style="background-color:#eeeeee"> <td>12</td> <td>5%</td>   </tr>
           <tr style="background-color:#ffffff"> <td>13</td> <td>4%</td>   </tr>
           <tr style="background-color:#eeeeee"> <td>14</td> <td>2.5%</td> </tr>
           <tr style="background-color:#ffffff"> <td>15</td> <td>2%</td>   </tr>
           <tr style="background-color:#eeeeee"> <td>16</td> <td>1.3%</td> </tr>
           <tr style="background-color:#ffffff"> <td>17</td> <td>1%</td>   </tr>
           <tr style="background-color:#eeeeee"> <td>18</td> <td>0.7%</td> </tr>
         </table>
         </td>
         <td>
           <table style="width:auto">
             <tr> <td colspan="7" class="strong">MinHash <code>k</code> @ 99% Confidence</td> </tr>
             <tr> <td rowspan="6" class="strong">Relative Error</td> <td>-</td> <td colspan="5" class="strong">Intersection Size</td> </tr>
             <tr style="background-color:#eeeeee"> <td>-</td>    <td>0.01%</td>  <td>0.1%</td>   <td>1.0%</td>  <td>5.0%</td>  <td>10.0%</td> </tr>
             <tr style="background-color:#ffffff"> <td>100%</td> <td>90000</td>  <td>9000</td>   <td>900</td>   <td>170</td>   <td>75</td>    </tr>
             <tr style="background-color:#eeeeee"> <td>50%</td>  <td>313334</td> <td>31334</td>  <td>3134</td>  <td>587</td>   <td>280</td>   </tr>
             <tr style="background-color:#ffffff"> <td>25%</td>  <td>-</td>      <td>116800</td> <td>11520</td> <td>2208</td>  <td>1040</td>  </tr>
             <tr style="background-color:#eeeeee"> <td>10%</td>  <td>-</td>      <td>-</td>      <td>68455</td> <td>13128</td> <td>6210</td>  </tr>
           </table>
         </td>
       </tr>
     </table>   
*/
package com.adroll.cantor;