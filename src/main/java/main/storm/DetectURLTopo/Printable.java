package main.storm.DetectURLTopo;

public class Printable {

	//0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ!"#$%&\'()*+,-./:;<=>?@[\\]^_`{|}~ \t\n\r\x0b\x0c
	//0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ!"#$%&\'()*+,-./:;<=>?@[\ \] ^ _ ` { | } ~  \t \n \r \x0b \x0c
	public int[] result;
	static String printable = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ!\"#$%&\\'()*+,-./:;<=>?@[\\\\]^_`{|}~ \\t\\n\\r\\x0b\\x0c";
//
//				0
//				1
//				2
//				3
//				4
//				5
//				6
//				7
//				8
//				9
//				a
//				b
//				c
//				d
//				e
//				f
//				g
//				h
//				i
//				j
//				k
//				l
//				m
//				n
//				o
//				p
//				q
//				r
//				s
//				t
//				u
//				v
//				w
//				x
//				y
//				z
//				A
//				B
//				C
//				D
//				E
//				F
//				G
//				H
//				I
//				J
//				K
//				L
//				M
//				N
//				O
//				P
//				Q
//				R
//				S
//				T
//				U
//				V
//				W
//				X
//				Y
//				Z
//				!
//				"
//				#
//				$
//				%
//				&
//				'
//				(
//				)
//				*
//				+
//				,
//				-
//				.
//				/
//				:
//				;
//				<
//				=
//				>
//				?
//				@
//				[
//				\
//				]
//				^
//				_
//				`
//				{
//				|
//				}
//				~

	
	
	public void covert(String url) {
		
		
		
	}
	
	public static void main(String[] args) {
		String inputURL = "http://www.naver.com";
		
		for(int i=0; i<printable.length(); i++) {
			System.out.println(printable.charAt(i));
		}
	}
}
