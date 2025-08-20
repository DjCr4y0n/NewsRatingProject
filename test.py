from google import genai

client = genai.Client(api_key="AIzaSyDS7jEPic1DaBGeCrALyhTzVdapSmXkj-M")

prompt = """
Given the following list of companies from the WIG20 index:
1. mbank
2. budimex
3. sanpl
4. ccc
5. kety
6. kghm
7. lpp
8. cdprojekt
9. pekao
10. pknorlen
11. pkobp
12. orangepl
13. pge
14. pzu
15. kruk
16. alior
17. dinopl
18. pepco
19. zabka
20. allegro

Analyze the following text and, in a single word, identify which of the listed companies the text is about, either directly or indirectly. If the text does not relate to any of these companies, respond with "Nan".

Text: 
Sytuację na krajowym rynku węgla energetycznego obrazują dwa indeksy, bazujące na danych miesięcznych ex-post i wyrażające cenę zbytu węgla kamiennego, w jakości dostosowanej do potrzeb odbiorców. Indeks PSCMI1 (z ang. Polish Steam Coal Market Index) wyraża ceny węgla dla tzw. energetyki zawodowej i przemysłowej, indeks PSCMI2 - dla ciepłowni przemysłowych i komunalnych. Wyrażona w indeksach wartość to cena węgla netto "na wagonie" w punkcie załadunku - bez uwzględnienia podatku akcyzowego, kosztów ubezpieczenia oraz kosztów dostawy.Indeks PSCMI1 w czerwcu 2025 r. pogorszył swój wynik o 1,9 proc. w porównaniu z poprzednim miesiącem i wyniósł 348,32 PLN/t. W porównaniu z czerwcem 2024 r. wynik ten jest gorszy o 27,5 proc.Indeks PSCMI2 w czerwcu 2025 r. pogorszył swój wynik o 1,6 proc. w porównaniu z poprzednim miesiącem i wyniósł 461,97 PLN/t. W porównaniu z czerwcem 2024 r. wynik ten jest gorszy o 19,1 proc.ARP podaje, że średnia wartość indeksu PSCMI1 dla II kwartału 2025 r. wyniosła 351,95 PLN/t. W porównaniu z II kwartałem poprzedniego roku zmalał on o 28,1 proc., a w porównaniu kdk zmalał o 2,7 proc.Średnia wartość indeksu PSCMI2 dla II kwartału 2025 r. wyniosła 470,25 PLN/t. W porównaniu z II kwartałem poprzedniego roku zmalał on o 17,0 proc., a w porównaniu kdk spadło o 3,3 proc. (PAP Biznes)map/ asa/

"""


response = client.models.generate_content(
    model="gemini-2.5-flash",
    contents=prompt,
)

print(response.text)





