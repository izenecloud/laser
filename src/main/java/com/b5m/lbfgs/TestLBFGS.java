package com.b5m.lbfgs;
/* copyright 2002 by Robert Dodier
*
* This program is free software; you can redistribute it and/or modify
* it under either option (1) or (2) below.
* (1) The GNU General Public License as published by the Free Software
* Foundation; either version 2 of the License, or (at your option)
* any later version.
* (2) The Apache license, version 2.
*/


public class TestLBFGS
{
	static int ndim = 2000 , msave = 7 ;
	static int nwork = ndim * ( 2 * msave + 1 ) + 2 * msave ;

	public static void main( String args[] )
	{
		double x [ ] , g [ ] , diag [ ] , w [ ];
		x = new double [ ndim ];
		g = new double [ ndim ];
		diag = new double [ ndim ];
		w = new double [ nwork ];

		double f, eps, xtol, gtol, t1, t2, stpmin, stpmax;
		int iprint [ ] , iflag[] = new int[1], icall, n, m, mp, lp, j;
		iprint = new int [ 2 ];
		boolean diagco;
		
		n=100;
		m=5;
		iprint [ 1 -1] = 3;
		iprint [ 2 -1] = 3;
		diagco= false;
		eps= 1.0e-5;
		xtol= 1.0e-16;
		icall=0;
		iflag[0]=0;

		for ( j = 1 ; j <= n ; j += 2 )
		{
			x [ j -1] = - 1.2e0;
			x [ j + 1 -1] = 1.e0;
		}

		do
		{
			f= 0;
			System.out.println("\nITER:ICALL TO CHANGE");
			for ( j = 1 ; j <= n ; j += 2 )
			{
				t1 = 1.e0 - x [ j -1];
				t2 = 1.e1 * ( x [ j + 1 -1] - x [ j -1] * x[j-1] );
				g [ j + 1 -1] = 2.e1 * t2;
				g [ j -1] = - 2.e0 * ( x [ j -1] * g [ j + 1 -1] + t1 );
				f= f+t1*t1+t2*t2;
			}

			try
			{
				LBFGS.lbfgs ( n , m , x , f , g , diagco , diag , iprint , eps , xtol , iflag );
			}
			catch (LBFGS.ExceptionWithIflag e)
			{
				System.err.println( "Sdrive: lbfgs failed.\n"+e );
				return;
			}
			System.out.println("\nITER:ICALL"+icall);
			icall += 1;
		}
		while ( iflag[0] != 0 && icall <= 200 );
	}
}
